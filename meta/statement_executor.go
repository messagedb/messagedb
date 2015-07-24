package meta

import (
	"fmt"

	"github.com/messagedb/messagedb/sql"
)

// StatementExecutor translates sql queries to meta store methods.
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
		SetPrivilege(username, database string, p sql.Privilege) error
		SetAdminPrivilege(username string, admin bool) error
		UserPrivileges(username string) (map[string]sql.Privilege, error)
		UserPrivilege(username, database string) (*sql.Privilege, error)

		CreateContinuousQuery(database, name, query string) error
		DropContinuousQuery(database, name string) error
	}
}

// ExecuteStatement executes stmt against the meta store as user.
func (e *StatementExecutor) ExecuteStatement(stmt sql.Statement) *sql.Result {
	switch stmt := stmt.(type) {
	case *sql.CreateDatabaseStatement:
		return e.executeCreateDatabaseStatement(stmt)
	case *sql.DropDatabaseStatement:
		return e.executeDropDatabaseStatement(stmt)
	case *sql.ShowDatabasesStatement:
		return e.executeShowDatabasesStatement(stmt)
	case *sql.ShowGrantsForUserStatement:
		return e.executeShowGrantsForUserStatement(stmt)
	case *sql.ShowServersStatement:
		return e.executeShowServersStatement(stmt)
	case *sql.CreateUserStatement:
		return e.executeCreateUserStatement(stmt)
	case *sql.SetPasswordUserStatement:
		return e.executeSetPasswordUserStatement(stmt)
	case *sql.DropUserStatement:
		return e.executeDropUserStatement(stmt)
	case *sql.ShowUsersStatement:
		return e.executeShowUsersStatement(stmt)
	case *sql.GrantStatement:
		return e.executeGrantStatement(stmt)
	case *sql.GrantAdminStatement:
		return e.executeGrantAdminStatement(stmt)
	case *sql.RevokeStatement:
		return e.executeRevokeStatement(stmt)
	case *sql.RevokeAdminStatement:
		return e.executeRevokeAdminStatement(stmt)
	case *sql.CreateRetentionPolicyStatement:
		return e.executeCreateRetentionPolicyStatement(stmt)
	case *sql.AlterRetentionPolicyStatement:
		return e.executeAlterRetentionPolicyStatement(stmt)
	case *sql.DropRetentionPolicyStatement:
		return e.executeDropRetentionPolicyStatement(stmt)
	case *sql.ShowRetentionPoliciesStatement:
		return e.executeShowRetentionPoliciesStatement(stmt)
	case *sql.CreateContinuousQueryStatement:
		return e.executeCreateContinuousQueryStatement(stmt)
	case *sql.DropContinuousQueryStatement:
		return e.executeDropContinuousQueryStatement(stmt)
	case *sql.ShowContinuousQueriesStatement:
		return e.executeShowContinuousQueriesStatement(stmt)
	case *sql.ShowStatsStatement:
		return e.executeShowStatsStatement(stmt)
	default:
		panic(fmt.Sprintf("unsupported statement type: %T", stmt))
	}
}

func (e *StatementExecutor) executeCreateDatabaseStatement(q *sql.CreateDatabaseStatement) *sql.Result {
	_, err := e.Store.CreateDatabase(q.Name)
	return &sql.Result{Err: err}
}

func (e *StatementExecutor) executeDropDatabaseStatement(q *sql.DropDatabaseStatement) *sql.Result {
	return &sql.Result{Err: e.Store.DropDatabase(q.Name)}
}

func (e *StatementExecutor) executeShowDatabasesStatement(q *sql.ShowDatabasesStatement) *sql.Result {
	dis, err := e.Store.Databases()
	if err != nil {
		return &sql.Result{Err: err}
	}

	row := &sql.Row{Name: "databases", Columns: []string{"name"}}
	for _, di := range dis {
		row.Values = append(row.Values, []interface{}{di.Name})
	}
	return &sql.Result{Series: []*sql.Row{row}}
}

func (e *StatementExecutor) executeShowGrantsForUserStatement(q *sql.ShowGrantsForUserStatement) *sql.Result {
	priv, err := e.Store.UserPrivileges(q.Name)
	if err != nil {
		return &sql.Result{Err: err}
	}

	row := &sql.Row{Columns: []string{"database", "privilege"}}
	for d, p := range priv {
		row.Values = append(row.Values, []interface{}{d, p.String()})
	}
	return &sql.Result{Series: []*sql.Row{row}}
}

func (e *StatementExecutor) executeShowServersStatement(q *sql.ShowServersStatement) *sql.Result {
	nis, err := e.Store.Nodes()
	if err != nil {
		return &sql.Result{Err: err}
	}

	row := &sql.Row{Columns: []string{"id", "url"}}
	for _, ni := range nis {
		row.Values = append(row.Values, []interface{}{ni.ID, "http://" + ni.Host})
	}
	return &sql.Result{Series: []*sql.Row{row}}
}

func (e *StatementExecutor) executeCreateUserStatement(q *sql.CreateUserStatement) *sql.Result {
	_, err := e.Store.CreateUser(q.Name, q.Password, q.Admin)
	return &sql.Result{Err: err}
}

func (e *StatementExecutor) executeSetPasswordUserStatement(q *sql.SetPasswordUserStatement) *sql.Result {
	return &sql.Result{Err: e.Store.UpdateUser(q.Name, q.Password)}
}

func (e *StatementExecutor) executeDropUserStatement(q *sql.DropUserStatement) *sql.Result {
	return &sql.Result{Err: e.Store.DropUser(q.Name)}
}

func (e *StatementExecutor) executeShowUsersStatement(q *sql.ShowUsersStatement) *sql.Result {
	uis, err := e.Store.Users()
	if err != nil {
		return &sql.Result{Err: err}
	}

	row := &sql.Row{Columns: []string{"user", "admin"}}
	for _, ui := range uis {
		row.Values = append(row.Values, []interface{}{ui.Name, ui.Admin})
	}
	return &sql.Result{Series: []*sql.Row{row}}
}

func (e *StatementExecutor) executeGrantStatement(stmt *sql.GrantStatement) *sql.Result {
	return &sql.Result{Err: e.Store.SetPrivilege(stmt.User, stmt.On, stmt.Privilege)}
}

func (e *StatementExecutor) executeGrantAdminStatement(stmt *sql.GrantAdminStatement) *sql.Result {
	return &sql.Result{Err: e.Store.SetAdminPrivilege(stmt.User, true)}
}

func (e *StatementExecutor) executeRevokeStatement(stmt *sql.RevokeStatement) *sql.Result {
	priv := sql.NoPrivileges

	// Revoking all privileges means there's no need to look at existing user privileges.
	if stmt.Privilege != sql.AllPrivileges {
		p, err := e.Store.UserPrivilege(stmt.User, stmt.On)
		if err != nil {
			return &sql.Result{Err: err}
		}
		// Bit clear (AND NOT) the user's privilege with the revoked privilege.
		priv = *p &^ stmt.Privilege
	}

	return &sql.Result{Err: e.Store.SetPrivilege(stmt.User, stmt.On, priv)}
}

func (e *StatementExecutor) executeRevokeAdminStatement(stmt *sql.RevokeAdminStatement) *sql.Result {
	return &sql.Result{Err: e.Store.SetAdminPrivilege(stmt.User, false)}
}

func (e *StatementExecutor) executeCreateRetentionPolicyStatement(stmt *sql.CreateRetentionPolicyStatement) *sql.Result {
	rpi := NewRetentionPolicyInfo(stmt.Name)
	rpi.Duration = stmt.Duration
	rpi.ReplicaN = stmt.Replication

	// Create new retention policy.
	_, err := e.Store.CreateRetentionPolicy(stmt.Database, rpi)
	if err != nil {
		return &sql.Result{Err: err}
	}

	// If requested, set new policy as the default.
	if stmt.Default {
		err = e.Store.SetDefaultRetentionPolicy(stmt.Database, stmt.Name)
	}

	return &sql.Result{Err: err}
}

func (e *StatementExecutor) executeAlterRetentionPolicyStatement(stmt *sql.AlterRetentionPolicyStatement) *sql.Result {
	rpu := &RetentionPolicyUpdate{
		Duration: stmt.Duration,
		ReplicaN: stmt.Replication,
	}

	// Update the retention policy.
	err := e.Store.UpdateRetentionPolicy(stmt.Database, stmt.Name, rpu)
	if err != nil {
		return &sql.Result{Err: err}
	}

	// If requested, set as default retention policy.
	if stmt.Default {
		err = e.Store.SetDefaultRetentionPolicy(stmt.Database, stmt.Name)
	}

	return &sql.Result{Err: err}
}

func (e *StatementExecutor) executeDropRetentionPolicyStatement(q *sql.DropRetentionPolicyStatement) *sql.Result {
	return &sql.Result{Err: e.Store.DropRetentionPolicy(q.Database, q.Name)}
}

func (e *StatementExecutor) executeShowRetentionPoliciesStatement(q *sql.ShowRetentionPoliciesStatement) *sql.Result {
	di, err := e.Store.Database(q.Database)
	if err != nil {
		return &sql.Result{Err: err}
	} else if di == nil {
		return &sql.Result{Err: ErrDatabaseNotFound}
	}

	row := &sql.Row{Columns: []string{"name", "duration", "replicaN", "default"}}
	for _, rpi := range di.RetentionPolicies {
		row.Values = append(row.Values, []interface{}{rpi.Name, rpi.Duration.String(), rpi.ReplicaN, di.DefaultRetentionPolicy == rpi.Name})
	}
	return &sql.Result{Series: []*sql.Row{row}}
}

func (e *StatementExecutor) executeCreateContinuousQueryStatement(q *sql.CreateContinuousQueryStatement) *sql.Result {
	return &sql.Result{
		Err: e.Store.CreateContinuousQuery(q.Database, q.Name, q.String()),
	}
}

func (e *StatementExecutor) executeDropContinuousQueryStatement(q *sql.DropContinuousQueryStatement) *sql.Result {
	return &sql.Result{
		Err: e.Store.DropContinuousQuery(q.Database, q.Name),
	}
}

func (e *StatementExecutor) executeShowContinuousQueriesStatement(stmt *sql.ShowContinuousQueriesStatement) *sql.Result {
	dis, err := e.Store.Databases()
	if err != nil {
		return &sql.Result{Err: err}
	}

	rows := []*sql.Row{}
	for _, di := range dis {
		row := &sql.Row{Columns: []string{"name", "query"}, Name: di.Name}
		for _, cqi := range di.ContinuousQueries {
			row.Values = append(row.Values, []interface{}{cqi.Name, cqi.Query})
		}
		rows = append(rows, row)
	}
	return &sql.Result{Series: rows}
}

func (e *StatementExecutor) executeShowStatsStatement(stmt *sql.ShowStatsStatement) *sql.Result {
	return &sql.Result{Err: fmt.Errorf("SHOW STATS is not implemented yet")}
}
