package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/messagedb/messagedb/messageql"
	"github.com/messagedb/messagedb/meta"
)

// QueryExecutor executes every statement in an influxdb Query. It is responsible for
// coordinating between the local tsdb.Store, the meta.Store, and the other nodes in
// the cluster to run the query against their local tsdb.Stores. There should be one executor
// in a running process
type QueryExecutor struct {
	// The meta store for accessing and updating cluster and schema data.
	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Databases() ([]meta.DatabaseInfo, error)
		User(name string) (*meta.UserInfo, error)
		AdminUserExists() (bool, error)
		Authenticate(username, password string) (*meta.UserInfo, error)
		RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
		UserCount() (int, error)
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
		NodeID() uint64
	}

	// Executes statements relating to meta data.
	MetaStatementExecutor interface {
		ExecuteStatement(stmt messageql.Statement) *messageql.Result
	}

	// Maps shards for queries.
	ShardMapper interface {
		CreateMapper(shard meta.ShardInfo, stmt string, chunkSize int) (Mapper, error)
	}

	Logger *log.Logger

	// the local data store
	store *Store
}

// NewQueryExecutor returns an initialized QueryExecutor
func NewQueryExecutor(store *Store) *QueryExecutor {
	return &QueryExecutor{
		store:  store,
		Logger: log.New(os.Stderr, "[query] ", log.LstdFlags),
	}
}

// Begin is for messageql/engine.go to use to get a transaction object to start the query
// func (q *QueryExecutor) Begin() (messageql.Tx, error) {
// 	return newTx(q.MetaStore, q.store), nil
// }

// Authorize user u to execute query q on database.
// database can be "" for queries that do not require a database.
// If no user is provided it will return an error unless the query's first statement is to create
// a root user.
func (q *QueryExecutor) Authorize(u *meta.UserInfo, query *messageql.Query, database string) error {
	const authErrLogFmt = "unauthorized request | user: %q | query: %q | database %q\n"

	// Special case if no users exist.
	if count, err := q.MetaStore.UserCount(); count == 0 && err == nil {
		// Get the first statement in the query.
		stmt := query.Statements[0]
		// First statement must create a root user.
		if cu, ok := stmt.(*messageql.CreateUserStatement); !ok ||
			cu.Privilege == nil ||
			*cu.Privilege != messageql.AllPrivileges {
			return ErrAuthorize{text: "no users exist. create root user first or disable authentication"}
		}
		return nil
	}

	if u == nil {
		q.Logger.Printf(authErrLogFmt, "", query.String(), database)
		return ErrAuthorize{text: "no user provided"}
	}

	// Cluster admins can do anything.
	if u.Admin {
		return nil
	}

	// Check each statement in the query.
	for _, stmt := range query.Statements {
		// Get the privileges required to execute the statement.
		privs := stmt.RequiredPrivileges()

		// Make sure the user has each privilege required to execute
		// the statement.
		for _, p := range privs {
			// Use the db name specified by the statement or the db
			// name passed by the caller if one wasn't specified by
			// the statement.
			dbname := p.Name
			if dbname == "" {
				dbname = database
			}

			// Check if user has required privilege.
			if !u.Authorize(p.Privilege, dbname) {
				var msg string
				if dbname == "" {
					msg = "requires cluster admin"
				} else {
					msg = fmt.Sprintf("requires %s privilege on %s", p.Privilege.String(), dbname)
				}
				q.Logger.Printf(authErrLogFmt, u.Name, query.String(), database)
				return ErrAuthorize{
					text: fmt.Sprintf("%s not authorized to execute '%s'.  %s", u.Name, stmt.String(), msg),
				}
			}
		}
	}
	return nil
}

// ExecuteQuery executes an InfluxQL query against the server.
// It sends results down the passed in chan and closes it when done. It will close the chan
// on the first statement that throws an error.
func (q *QueryExecutor) ExecuteQuery(query *messageql.Query, database string, chunkSize int) (<-chan *messageql.Result, error) {
	// Execute each statement. Keep the iterator external so we can
	// track how many of the statements were executed
	results := make(chan *messageql.Result)
	go func() {
		var i int
		var stmt messageql.Statement
		for i, stmt = range query.Statements {
			// If a default database wasn't passed in by the caller, check the statement.
			// Some types of statements have an associated default database, even if it
			// is not explicitly included.
			defaultDB := database
			if defaultDB == "" {
				if s, ok := stmt.(messageql.HasDefaultDatabase); ok {
					defaultDB = s.DefaultDatabase()
				}
			}

			// Normalize each statement.
			if err := q.normalizeStatement(stmt, defaultDB); err != nil {
				results <- &messageql.Result{Err: err}
				break
			}

			// func (*DropConversationStatement) stmt()    {}
			// func (*DropDatabaseStatement) stmt()        {}
			// func (*DropOrganizationStatement) stmt()    {}
			// func (*DropRetentionPolicyStatement) stmt() {}
			// func (*DropUserStatement) stmt()            {}

			var res *messageql.Result
			switch stmt := stmt.(type) {
			case *messageql.SelectStatement:
				if err := q.executeSelectStatement(i, stmt, results, chunkSize); err != nil {
					results <- &messageql.Result{Err: err}
					break
				}
			case *messageql.DropConversationStatement:
				// TODO: handle this in a cluster
				res = q.executeDropConversationStatement(stmt, database)
			case *messageql.ShowConversationsStatement:
				res = q.executeShowConversationsStatement(stmt, database)
			case *messageql.ShowDiagnosticsStatement:
				res = q.executeShowDiagnosticsStatement(stmt)
			case *messageql.DeleteStatement:
				res = &messageql.Result{Err: ErrInvalidQuery}
			case *messageql.DropDatabaseStatement:
				// TODO: handle this in a cluster
				res = q.executeDropDatabaseStatement(stmt)
			default:
				// Delegate all other meta statements to a separate executor. They don't hit tsdb storage.
				res = q.MetaStatementExecutor.ExecuteStatement(stmt)
			}

			if res != nil {
				// set the StatementID for the handler on the other side to combine results
				res.StatementID = i

				// If an error occurs then stop processing remaining statements.
				results <- res
				if res.Err != nil {
					break
				}
			}
		}

		// if there was an error send results that the remaining statements weren't executed
		for ; i < len(query.Statements)-1; i++ {
			results <- &messageql.Result{Err: ErrNotExecuted}
		}

		close(results)
	}()

	return results, nil
}

// Plan creates an execution plan for the given SelectStatement and returns an Executor.
func (q *QueryExecutor) plan(stmt *messageql.SelectStatement, chunkSize int) (Executor, error) {
	shards := map[uint64]meta.ShardInfo{} // Shards requiring mappers.

	// Replace instances of "now()" with the current time, and check the resultant times.
	stmt.Condition = messageql.Reduce(stmt.Condition, &messageql.NowValuer{Now: time.Now().UTC()})
	tmin, tmax := messageql.TimeRange(stmt.Condition)
	if tmax.IsZero() {
		tmax = time.Now()
	}
	if tmin.IsZero() {
		tmin = time.Unix(0, 0)
	}

	for _, src := range stmt.Sources {
		mm, ok := src.(*messageql.Conversation)
		if !ok {
			return nil, fmt.Errorf("invalid source type: %#v", src)
		}

		// Build the set of target shards. Using shard IDs as keys ensures each shard ID
		// occurs only once.
		shardGroups, err := q.MetaStore.ShardGroupsByTimeRange(mm.Database, mm.RetentionPolicy, tmin, tmax)
		if err != nil {
			return nil, err
		}
		for _, g := range shardGroups {
			for _, sh := range g.Shards {
				shards[sh.ID] = sh
			}
		}
	}

	// Build the Mappers, one per shard.
	mappers := []Mapper{}
	for _, sh := range shards {
		m, err := q.ShardMapper.CreateMapper(sh, stmt.String(), chunkSize)
		if err != nil {
			return nil, err
		}
		if m == nil {
			// No data for this shard, skip it.
			continue
		}
		mappers = append(mappers, m)
	}

	var executor Executor
	if len(mappers) > 0 {
		executor = NewRawExecutor(stmt, mappers, chunkSize)
	} else {
		executor = NewRawExecutor(stmt, nil, chunkSize)
	}

	// if stmt.IsRawQuery && !stmt.HasDistinct() {
	// 	return NewRawExecutor(stmt, mappers, chunkSize), nil
	// }
	// return NewAggregateExecutor(stmt, mappers), nil
	return executor, nil
}

// executeSelectStatement plans and executes a select statement against a database.
func (q *QueryExecutor) executeSelectStatement(statementID int, stmt *messageql.SelectStatement, results chan *messageql.Result, chunkSize int) error {
	// Perform any necessary query re-writing.
	stmt, err := q.rewriteSelectStatement(stmt)
	if err != nil {
		return err
	}

	// Plan statement execution.
	e, err := q.plan(stmt, chunkSize)
	if err != nil {
		return err
	}

	// Execute plan.
	ch := e.Execute()

	// Stream results from the channel. We should send an empty result if nothing comes through.
	resultSent := false
	for row := range ch {
		if row.Err != nil {
			return row.Err
		}
		resultSent = true
		results <- &messageql.Result{StatementID: statementID, Rows: []*messageql.Row{row}}
	}

	if !resultSent {
		results <- &messageql.Result{StatementID: statementID, Rows: make([]*messageql.Row, 0)}
	}

	return nil
}

// rewriteSelectStatement performs any necessary query re-writing.
func (q *QueryExecutor) rewriteSelectStatement(stmt *messageql.SelectStatement) (*messageql.SelectStatement, error) {
	var err error

	// Expand regex expressions in the FROM clause.
	sources, err := q.expandSources(stmt.Sources)
	if err != nil {
		return nil, err
	}
	stmt.Sources = sources

	// Expand wildcards in the fields or GROUP BY.
	// if stmt.HasWildcard() {
	// 	stmt, err = q.expandWildcards(stmt)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	// stmt.RewriteDistinct()

	return stmt, nil
}

// expandSources expands regex sources and removes duplicates.
// NOTE: sources must be normalized (db and rp set) before calling this function.
func (q *QueryExecutor) expandSources(sources messageql.Sources) (messageql.Sources, error) {
	// Use a map as a set to prevent duplicates. Two regexes might produce
	// duplicates when expanded.
	set := map[string]messageql.Source{}
	names := []string{}

	// Iterate all sources, expanding regexes when they're found.
	for _, source := range sources {
		switch src := source.(type) {
		case *messageql.Conversation:
			if src.Regex == nil {
				name := src.String()
				set[name] = src
				names = append(names, name)
				continue
			}

			// Lookup the database.
			db := q.store.DatabaseIndex(src.Database)
			if db == nil {
				return nil, nil
			}

			// Get conversations from the database that match the regex.
			conversations := db.conversationsByRegex(src.Regex.Val)

			// Add those conversations to the set.
			for _, m := range conversations {
				m2 := &messageql.Conversation{
					Database:        src.Database,
					RetentionPolicy: src.RetentionPolicy,
					Name:            m.Name,
				}

				name := m2.String()
				if _, ok := set[name]; !ok {
					set[name] = m2
					names = append(names, name)
				}
			}

		default:
			return nil, fmt.Errorf("expandSources: unsuported source type: %T", source)
		}
	}

	// Sort the list of source names.
	sort.Strings(names)

	// Convert set to a list of Sources.
	expanded := make(messageql.Sources, 0, len(set))
	for _, name := range names {
		expanded = append(expanded, set[name])
	}

	return expanded, nil
}

// executeDropDatabaseStatement closes all local shards for the database and removes the directory. It then calls to the metastore to remove the database from there.
// TODO: make this work in a cluster/distributed
func (q *QueryExecutor) executeDropDatabaseStatement(stmt *messageql.DropDatabaseStatement) *messageql.Result {
	dbi, err := q.MetaStore.Database(stmt.Name)
	if err != nil {
		return &messageql.Result{Err: err}
	} else if dbi == nil {
		return &messageql.Result{Err: ErrDatabaseNotFound(stmt.Name)}
	}

	var shardIDs []uint64
	for _, rp := range dbi.RetentionPolicies {
		for _, sg := range rp.ShardGroups {
			for _, s := range sg.Shards {
				shardIDs = append(shardIDs, s.ID)
			}
		}
	}

	err = q.store.DeleteDatabase(stmt.Name, shardIDs)
	if err != nil {
		return &messageql.Result{Err: err}
	}

	return q.MetaStatementExecutor.ExecuteStatement(stmt)
}

// executeDropConversationStatement removes all series from the local store that match the drop query
func (q *QueryExecutor) executeDropConversationStatement(stmt *messageql.DropConversationStatement, database string) *messageql.Result {
	// Find the database.
	db := q.store.DatabaseIndex(database)
	if db == nil {
		return &messageql.Result{}
	}

	m := db.Conversation(stmt.Name)
	if m == nil {
		return &messageql.Result{Err: ErrConversationNotFound(stmt.Name)}
	}

	db.DropConversation(m.Name)

	if err := q.store.deleteConversation(m.Name); err != nil {
		return &messageql.Result{Err: err}
	}

	return &messageql.Result{}
}

func (q *QueryExecutor) executeShowConversationsStatement(stmt *messageql.ShowConversationsStatement, database string) *messageql.Result {
	// Find the database.
	db := q.store.DatabaseIndex(database)
	if db == nil {
		return &messageql.Result{}
	}

	sources, err := q.expandSources(stmt.Sources)
	if err != nil {
		return &messageql.Result{Err: err}
	}

	// Get the list of measurements we're interested in.
	conversations, err := conversationsFromSourcesOrDB(db, sources...)
	if err != nil {
		return &messageql.Result{Err: err}
	}

	sort.Sort(conversations)

	offset := stmt.Offset
	limit := stmt.Limit

	// If OFFSET is past the end of the array, return empty results.
	if offset > len(conversations)-1 {
		return &messageql.Result{}
	}

	// Calculate last index based on LIMIT.
	end := len(conversations)
	if limit > 0 && offset+limit < end {
		limit = offset + limit
	} else {
		limit = end
	}

	// Make a result row to hold all measurement names.
	row := &messageql.Row{
		Name:    "conversations",
		Columns: []string{"name"},
	}

	// Add one value to the row for each measurement name.
	for i := offset; i < limit; i++ {
		m := conversations[i]
		v := interface{}(m.Name)
		row.Values = append(row.Values, []interface{}{v})
	}

	// Make a result.
	result := &messageql.Result{
		Rows: []*messageql.Row{row},
	}

	return result
}

// conversationsFromSourcesOrDB returns a list of conversations from the
// sources passed in or, if sources is empty, a list of all
// conversations names from the database passed in.
func conversationsFromSourcesOrDB(db *DatabaseIndex, sources ...messageql.Source) (Conversations, error) {
	var conversations Conversations
	if len(sources) > 0 {
		for _, source := range sources {
			if c, ok := source.(*messageql.Conversation); ok {
				conversation := db.conversations[c.Name]
				if conversation == nil {
					return nil, ErrConversationNotFound(c.Name)
				}

				conversations = append(conversations, conversation)
			} else {
				return nil, errors.New("identifiers in FROM clause must be conversation names")
			}
		}
	} else {
		// No organizations specified in FROM clause so get all conversations in the database
		for _, c := range db.Conversations() {
			conversations = append(conversations, c)
		}
	}
	sort.Sort(conversations)

	return conversations, nil
}

// normalizeStatement adds a default database and policy to the measurements in statement.
func (q *QueryExecutor) normalizeStatement(stmt messageql.Statement, defaultDatabase string) (err error) {
	// Track prefixes for replacing field names.
	prefixes := make(map[string]string)

	// Qualify all measurements.
	messageql.WalkFunc(stmt, func(n messageql.Node) {
		if err != nil {
			return
		}
		switch n := n.(type) {
		case *messageql.Conversation:
			e := q.normalizeConversation(n, defaultDatabase)
			if e != nil {
				err = e
				return
			}
			prefixes[n.Name] = n.Name
		}
	})
	if err != nil {
		return err
	}

	// Replace all variable references that used measurement prefixes.
	messageql.WalkFunc(stmt, func(n messageql.Node) {
		switch n := n.(type) {
		case *messageql.VarRef:
			for k, v := range prefixes {
				if strings.HasPrefix(n.Val, k+".") {
					n.Val = v + "." + messageql.QuoteIdent(n.Val[len(k)+1:])
				}
			}
		}
	})

	return
}

// normalizeMeasurement inserts the default database or policy into all measurement names,
// if required.
func (q *QueryExecutor) normalizeConversation(m *messageql.Conversation, defaultDatabase string) error {
	if m.Name == "" && m.Regex == nil {
		return errors.New("invalid conversation")
	}

	// Measurement does not have an explicit database? Insert default.
	if m.Database == "" {
		m.Database = defaultDatabase
	}

	// The database must now be specified by this point.
	if m.Database == "" {
		return errors.New("database name required")
	}

	// Find database.
	di, err := q.MetaStore.Database(m.Database)
	if err != nil {
		return err
	} else if di == nil {
		return ErrDatabaseNotFound(m.Database)
	}

	// If no retention policy was specified, use the default.
	if m.RetentionPolicy == "" {
		if di.DefaultRetentionPolicy == "" {
			return fmt.Errorf("default retention policy not set for: %s", di.Name)
		}
		m.RetentionPolicy = di.DefaultRetentionPolicy
	}

	return nil
}

func (q *QueryExecutor) executeShowDiagnosticsStatement(stmt *messageql.ShowDiagnosticsStatement) *messageql.Result {
	return &messageql.Result{Err: fmt.Errorf("SHOW DIAGNOSTICS is not implemented yet")}
}

// ErrAuthorize represents an authorization error.
type ErrAuthorize struct {
	text string
}

// Error returns the text of the error.
func (e ErrAuthorize) Error() string {
	return e.text
}

// authorize satisfies isAuthorizationError
func (ErrAuthorize) authorize() {}

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")
)

func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

func ErrConversationNotFound(name string) error { return fmt.Errorf("conversation not found: %s", name) }
