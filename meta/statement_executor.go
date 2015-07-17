package meta

import (
	"fmt"

	"github.com/messagedb/messagedb/messageql"
)

// StatementExecutor translates InfluxQL queries to meta store methods.
type StatementExecutor struct {
	Store interface {
		Nodes() ([]NodeInfo, error)

		Database(name string) (*DatabaseInfo, error)
		Databases() ([]DatabaseInfo, error)
		CreateDatabase(name string) (*DatabaseInfo, error)
		DropDatabase(name string) error

		DefaultRetentionPolicy(database string) (*RetentionPolicyInfo, error)
		CreateRetentionPolicy(database string, rpi *RetentionPolicyInfo) (*RetentionPolicyInfo, error)
		UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) error
		SetDefaultRetentionPolicy(database, name string) error
		DropRetentionPolicy(database, name string) error

		Users() ([]UserInfo, error)
		CreateUser(name, password string, admin bool) (*UserInfo, error)
		UpdateUser(name, password string) error
		DropUser(name string) error
		SetPrivilege(username, database string, p messageql.Privilege) error
		UserPrivileges(username string) (map[string]messageql.Privilege, error)

		// CreateContinuousQuery(database, name, query string) error
		// DropContinuousQuery(database, name string) error
	}
}

// ExecuteStatement executes stmt against the meta store as user.
func (e *StatementExecutor) ExecuteStatement(stmt messageql.Statement) *messageql.Result {
	switch stmt := stmt.(type) {
	case *messageql.CreateDatabaseStatement:
		return e.executeCreateDatabaseStatement(stmt)
	case *messageql.DropDatabaseStatement:
		return e.executeDropDatabaseStatement(stmt)
	case *messageql.ShowDatabasesStatement:
		return e.executeShowDatabasesStatement(stmt)
	case *messageql.ShowGrantsForUserStatement:
		return e.executeShowGrantsForUserStatement(stmt)
	case *messageql.ShowServersStatement:
		return e.executeShowServersStatement(stmt)
	case *messageql.CreateUserStatement:
		return e.executeCreateUserStatement(stmt)
	case *messageql.SetPasswordUserStatement:
		return e.executeSetPasswordUserStatement(stmt)
	case *messageql.DropUserStatement:
		return e.executeDropUserStatement(stmt)
	case *messageql.ShowUsersStatement:
		return e.executeShowUsersStatement(stmt)
	case *messageql.GrantStatement:
		return e.executeGrantStatement(stmt)
	case *messageql.RevokeStatement:
		return e.executeRevokeStatement(stmt)
	case *messageql.CreateRetentionPolicyStatement:
		return e.executeCreateRetentionPolicyStatement(stmt)
	case *messageql.AlterRetentionPolicyStatement:
		return e.executeAlterRetentionPolicyStatement(stmt)
	case *messageql.DropRetentionPolicyStatement:
		return e.executeDropRetentionPolicyStatement(stmt)
	case *messageql.ShowRetentionPoliciesStatement:
		return e.executeShowRetentionPoliciesStatement(stmt)
	case *messageql.ShowStatsStatement:
		return e.executeShowStatsStatement(stmt)
	default:
		panic(fmt.Sprintf("unsupported statement type: %T", stmt))
	}
}

func (e *StatementExecutor) executeCreateDatabaseStatement(q *messageql.CreateDatabaseStatement) *messageql.Result {
	_, err := e.Store.CreateDatabase(q.Name)
	return &messageql.Result{Err: err}
}

func (e *StatementExecutor) executeDropDatabaseStatement(q *messageql.DropDatabaseStatement) *messageql.Result {
	return &messageql.Result{Err: e.Store.DropDatabase(q.Name)}
}

func (e *StatementExecutor) executeShowDatabasesStatement(q *messageql.ShowDatabasesStatement) *messageql.Result {
	dis, err := e.Store.Databases()
	if err != nil {
		return &messageql.Result{Err: err}
	}

	row := &messageql.Row{Name: "databases", Columns: []string{"name"}}
	for _, di := range dis {
		row.Values = append(row.Values, []interface{}{di.Name})
	}
	return &messageql.Result{Rows: []*messageql.Row{row}}
}

func (e *StatementExecutor) executeShowGrantsForUserStatement(q *messageql.ShowGrantsForUserStatement) *messageql.Result {
	priv, err := e.Store.UserPrivileges(q.Name)
	if err != nil {
		return &messageql.Result{Err: err}
	}

	row := &messageql.Row{Columns: []string{"database", "privilege"}}
	for d, p := range priv {
		row.Values = append(row.Values, []interface{}{d, p.String()})
	}
	return &messageql.Result{Rows: []*messageql.Row{row}}
}

func (e *StatementExecutor) executeShowServersStatement(q *messageql.ShowServersStatement) *messageql.Result {
	nis, err := e.Store.Nodes()
	if err != nil {
		return &messageql.Result{Err: err}
	}

	row := &messageql.Row{Columns: []string{"id", "url"}}
	for _, ni := range nis {
		row.Values = append(row.Values, []interface{}{ni.ID, "http://" + ni.Host})
	}
	return &messageql.Result{Rows: []*messageql.Row{row}}
}

func (e *StatementExecutor) executeCreateUserStatement(q *messageql.CreateUserStatement) *messageql.Result {
	admin := false
	if q.Privilege != nil {
		admin = (*q.Privilege == messageql.AllPrivileges)
	}

	_, err := e.Store.CreateUser(q.Name, q.Password, admin)
	return &messageql.Result{Err: err}
}

func (e *StatementExecutor) executeSetPasswordUserStatement(q *messageql.SetPasswordUserStatement) *messageql.Result {
	return &messageql.Result{Err: e.Store.UpdateUser(q.Name, q.Password)}
}

func (e *StatementExecutor) executeDropUserStatement(q *messageql.DropUserStatement) *messageql.Result {
	return &messageql.Result{Err: e.Store.DropUser(q.Name)}
}

func (e *StatementExecutor) executeShowUsersStatement(q *messageql.ShowUsersStatement) *messageql.Result {
	uis, err := e.Store.Users()
	if err != nil {
		return &messageql.Result{Err: err}
	}

	row := &messageql.Row{Columns: []string{"user", "admin"}}
	for _, ui := range uis {
		row.Values = append(row.Values, []interface{}{ui.Name, ui.Admin})
	}
	return &messageql.Result{Rows: []*messageql.Row{row}}
}

func (e *StatementExecutor) executeGrantStatement(stmt *messageql.GrantStatement) *messageql.Result {
	return &messageql.Result{Err: e.Store.SetPrivilege(stmt.User, stmt.On, stmt.Privilege)}
}

func (e *StatementExecutor) executeRevokeStatement(stmt *messageql.RevokeStatement) *messageql.Result {
	return &messageql.Result{Err: e.Store.SetPrivilege(stmt.User, stmt.On, messageql.NoPrivileges)}
}

func (e *StatementExecutor) executeCreateRetentionPolicyStatement(stmt *messageql.CreateRetentionPolicyStatement) *messageql.Result {
	rpi := NewRetentionPolicyInfo(stmt.Name)
	rpi.Duration = stmt.Duration
	rpi.ReplicaN = stmt.Replication

	// Create new retention policy.
	_, err := e.Store.CreateRetentionPolicy(stmt.Database, rpi)
	if err != nil {
		return &messageql.Result{Err: err}
	}

	// If requested, set new policy as the default.
	if stmt.Default {
		err = e.Store.SetDefaultRetentionPolicy(stmt.Database, stmt.Name)
	}

	return &messageql.Result{Err: err}
}

func (e *StatementExecutor) executeAlterRetentionPolicyStatement(stmt *messageql.AlterRetentionPolicyStatement) *messageql.Result {
	rpu := &RetentionPolicyUpdate{
		Duration: stmt.Duration,
		ReplicaN: stmt.Replication,
	}

	// Update the retention policy.
	err := e.Store.UpdateRetentionPolicy(stmt.Database, stmt.Name, rpu)
	if err != nil {
		return &messageql.Result{Err: err}
	}

	// If requested, set as default retention policy.
	if stmt.Default {
		err = e.Store.SetDefaultRetentionPolicy(stmt.Database, stmt.Name)
	}

	return &messageql.Result{Err: err}
}

func (e *StatementExecutor) executeDropRetentionPolicyStatement(q *messageql.DropRetentionPolicyStatement) *messageql.Result {
	return &messageql.Result{Err: e.Store.DropRetentionPolicy(q.Database, q.Name)}
}

func (e *StatementExecutor) executeShowRetentionPoliciesStatement(q *messageql.ShowRetentionPoliciesStatement) *messageql.Result {
	di, err := e.Store.Database(q.Database)
	if err != nil {
		return &messageql.Result{Err: err}
	} else if di == nil {
		return &messageql.Result{Err: ErrDatabaseNotFound}
	}

	row := &messageql.Row{Columns: []string{"name", "duration", "replicaN", "default"}}
	for _, rpi := range di.RetentionPolicies {
		row.Values = append(row.Values, []interface{}{rpi.Name, rpi.Duration.String(), rpi.ReplicaN, di.DefaultRetentionPolicy == rpi.Name})
	}
	return &messageql.Result{Rows: []*messageql.Row{row}}
}

func (e *StatementExecutor) executeShowStatsStatement(stmt *messageql.ShowStatsStatement) *messageql.Result {
	return &messageql.Result{Err: fmt.Errorf("SHOW STATS is not implemented yet")}
}
