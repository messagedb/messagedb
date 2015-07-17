package messageql

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Statement represents a single command in MessageQL.
type Statement interface {
	Node
	stmt()
	RequiredPrivileges() ExecutionPrivileges
}

// Statements represents a list of statements.
type Statements []Statement

// String returns a string representation of the statements.
func (a Statements) String() string {
	var str []string
	for _, stmt := range a {
		str = append(str, stmt.String())
	}
	return strings.Join(str, ";\n")
}

// Query represents a collection of ordered statements.
type Query struct {
	Statements Statements
}

// String returns a string representation of the query.
func (q *Query) String() string { return q.Statements.String() }

// HasDefaultDatabase provides an interface to get the default database from a Statement.
type HasDefaultDatabase interface {
	Node
	stmt()
	DefaultDatabase() string
}

// ExecutionPrivilege is a privilege required for a user to execute
// a statement on a database or resource.
type ExecutionPrivilege struct {
	// Name of the database or resource.
	// If "", then the resource is the cluster.
	Name string

	// Privilege required.
	Privilege Privilege
}

// ExecutionPrivileges is a list of privileges required to execute a statement.
type ExecutionPrivileges []ExecutionPrivilege

func (*AlterRetentionPolicyStatement) stmt() {}

// func (*CreateContinuousQueryStatement) stmt() {}
func (*CreateDatabaseStatement) stmt()        {}
func (*CreateRetentionPolicyStatement) stmt() {}
func (*CreateUserStatement) stmt()            {}
func (*DeleteStatement) stmt()                {}

func (*DropConversationStatement) stmt()    {}
func (*DropDatabaseStatement) stmt()        {}
func (*DropOrganizationStatement) stmt()    {}
func (*DropRetentionPolicyStatement) stmt() {}
func (*DropUserStatement) stmt()            {}

func (*ShowConversationsStatement) stmt()       {}
func (*ShowDatabasesStatement) stmt()           {}
func (*ShowDiagnosticsStatement) stmt()         {}
func (*ShowDevicesForUserStatement) stmt()      {}
func (*ShowGrantsForUserStatement) stmt()       {}
func (*ShowOrganizationsStatement) stmt()       {}
func (*ShowOrganizationMembersStatement) stmt() {}
func (*ShowRetentionPoliciesStatement) stmt()   {}
func (*ShowServersStatement) stmt()             {}
func (*ShowStatsStatement) stmt()               {}
func (*ShowUsersStatement) stmt()               {}

func (*GrantStatement) stmt()  {}
func (*RevokeStatement) stmt() {}

func (*SelectStatement) stmt() {}

func (*SetPasswordUserStatement) stmt() {}

// STATEMENTS

// CreateDatabaseStatement represents a command for creating a new database.
type CreateDatabaseStatement struct {
	// Name of the database to be created.
	Name string
}

// String returns a string representation of the create database statement.
func (s *CreateDatabaseStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("CREATE DATABASE ")
	_, _ = buf.WriteString(s.Name)
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a CreateDatabaseStatement.
func (s *CreateDatabaseStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// DropDatabaseStatement represents a command to drop a database.
type DropDatabaseStatement struct {
	// Name of the database to be dropped.
	Name string
}

// String returns a string representation of the drop database statement.
func (s *DropDatabaseStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP DATABASE ")
	_, _ = buf.WriteString(s.Name)
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a DropDatabaseStatement.
func (s *DropDatabaseStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// CreateUserStatement represents a command for creating a new user.
type CreateUserStatement struct {
	// Name of the user to be created.
	Name string

	// User's password
	Password string

	// User's privilege level.
	Privilege *Privilege
}

// String returns a string representation of the create user statement.
func (s *CreateUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("CREATE USER ")
	_, _ = buf.WriteString(s.Name)
	_, _ = buf.WriteString(" WITH PASSWORD ")
	_, _ = buf.WriteString(s.Password)

	if s.Privilege != nil {
		_, _ = buf.WriteString(" WITH ")
		_, _ = buf.WriteString(s.Privilege.String())
	}

	return buf.String()
}

// RequiredPrivileges returns the privilege(s) required to execute a CreateUserStatement.
func (s *CreateUserStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// DropUserStatement represents a command for dropping a user.
type DropUserStatement struct {
	// Name of the user to drop.
	Name string
}

// String returns a string representation of the drop user statement.
func (s *DropUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP USER ")
	_, _ = buf.WriteString(s.Name)
	return buf.String()
}

// RequiredPrivileges returns the privilege(s) required to execute a DropUserStatement.
func (s *DropUserStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// Privilege is a type of action a user can be granted the right to use.
type Privilege int

const (
	// NoPrivileges means no privileges required / granted / revoked.
	NoPrivileges Privilege = iota
	// ReadPrivilege means read privilege required / granted / revoked.
	ReadPrivilege
	// WritePrivilege means write privilege required / granted / revoked.
	WritePrivilege
	// AllPrivileges means all privileges required / granted / revoked.
	AllPrivileges
)

// NewPrivilege returns an initialized *Privilege.
func NewPrivilege(p Privilege) *Privilege { return &p }

// String returns a string representation of a Privilege.
func (p Privilege) String() string {
	switch p {
	case NoPrivileges:
		return "NO PRIVILEGES"
	case ReadPrivilege:
		return "READ"
	case WritePrivilege:
		return "WRITE"
	case AllPrivileges:
		return "ALL PRIVILEGES"
	}
	return ""
}

// GrantStatement represents a command for granting a privilege.
type GrantStatement struct {
	// The privilege to be granted.
	Privilege Privilege

	// Thing to grant privilege on (e.g., a DB).
	On string

	// Who to grant the privilege to.
	User string
}

// String returns a string representation of the grant statement.
func (s *GrantStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("GRANT ")
	_, _ = buf.WriteString(s.Privilege.String())
	if s.On != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(s.On)
	}
	_, _ = buf.WriteString(" TO ")
	_, _ = buf.WriteString(s.User)
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a GrantStatement.
func (s *GrantStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// SetPasswordUserStatement represents a command for changing user password.
type SetPasswordUserStatement struct {
	// Plain Password
	Password string

	// Who to grant the privilege to.
	Name string
}

// String returns a string representation of the set password statement.
func (s *SetPasswordUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SET PASSWORD FOR ")
	_, _ = buf.WriteString(s.Name)
	_, _ = buf.WriteString(" = ")
	_, _ = buf.WriteString(s.Password)
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a GrantStatement.
func (s *SetPasswordUserStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// RevokeStatement represents a command to revoke a privilege from a user.
type RevokeStatement struct {
	// Privilege to be revoked.
	Privilege Privilege

	// Thing to revoke privilege to (e.g., a DB)
	On string

	// Who to revoke privilege from.
	User string
}

// String returns a string representation of the revoke statement.
func (s *RevokeStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("REVOKE ")
	_, _ = buf.WriteString(s.Privilege.String())
	if s.On != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(s.On)
	}
	_, _ = buf.WriteString(" FROM ")
	_, _ = buf.WriteString(s.User)
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a RevokeStatement.
func (s *RevokeStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// CreateRetentionPolicyStatement represents a command to create a retention policy.
type CreateRetentionPolicyStatement struct {
	// Name of policy to create.
	Name string

	// Name of database this policy belongs to.
	Database string

	// Duration data written to this policy will be retained.
	Duration time.Duration

	// Replication factor for data written to this policy.
	Replication int

	// Should this policy be set as default for the database?
	Default bool
}

// String returns a string representation of the create retention policy.
func (s *CreateRetentionPolicyStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("CREATE RETENTION POLICY ")
	_, _ = buf.WriteString(s.Name)
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(s.Database)
	_, _ = buf.WriteString(" DURATION ")
	_, _ = buf.WriteString(FormatDuration(s.Duration))
	_, _ = buf.WriteString(" REPLICATION ")
	_, _ = buf.WriteString(strconv.Itoa(s.Replication))
	if s.Default {
		_, _ = buf.WriteString(" DEFAULT")
	}
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a CreateRetentionPolicyStatement.
func (s *CreateRetentionPolicyStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// AlterRetentionPolicyStatement represents a command to alter an existing retention policy.
type AlterRetentionPolicyStatement struct {
	// Name of policy to alter.
	Name string

	// Name of the database this policy belongs to.
	Database string

	// Duration data written to this policy will be retained.
	Duration *time.Duration

	// Replication factor for data written to this policy.
	Replication *int

	// Should this policy be set as defalut for the database?
	Default bool
}

// String returns a string representation of the alter retention policy statement.
func (s *AlterRetentionPolicyStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("ALTER RETENTION POLICY ")
	_, _ = buf.WriteString(s.Name)
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(s.Database)

	if s.Duration != nil {
		_, _ = buf.WriteString(" DURATION ")
		_, _ = buf.WriteString(FormatDuration(*s.Duration))
	}

	if s.Replication != nil {
		_, _ = buf.WriteString(" REPLICATION ")
		_, _ = buf.WriteString(strconv.Itoa(*s.Replication))
	}

	if s.Default {
		_, _ = buf.WriteString(" DEFAULT")
	}

	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute an AlterRetentionPolicyStatement.
func (s *AlterRetentionPolicyStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// DropRetentionPolicyStatement represents a command to drop a retention policy from a database.
type DropRetentionPolicyStatement struct {
	// Name of the policy to drop.
	Name string

	// Name of the database to drop the policy from.
	Database string
}

// String returns a string representation of the drop retention policy statement.
func (s *DropRetentionPolicyStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP RETENTION POLICY ")
	_, _ = buf.WriteString(s.Name)
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(s.Database)
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a DropRetentionPolicyStatement.
func (s *DropRetentionPolicyStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: s.Database, Privilege: WritePrivilege}}
}

// SelectStatement represents a command for extracting data from the database.
type SelectStatement struct {
	// Expressions returned from the selection.
	Fields Fields

	// Expressions used for grouping the selection.
	Dimensions Dimensions

	// Data sources that fields are extracted from.
	Sources Sources

	// An expression evaluated on data point.
	Condition Expr

	// Fields to sort results by
	SortFields SortFields

	// Maximum number of rows to be returned. Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int

	// if it's a query for raw data values (i.e. not an aggregate)
	IsRawQuery bool
}

// Clone returns a deep copy of the statement.
func (s *SelectStatement) Clone() *SelectStatement {
	clone := &SelectStatement{
		Fields:     make(Fields, 0, len(s.Fields)),
		Dimensions: make(Dimensions, 0, len(s.Dimensions)),
		Sources:    cloneSources(s.Sources),
		SortFields: make(SortFields, 0, len(s.SortFields)),
		Condition:  CloneExpr(s.Condition),
		Limit:      s.Limit,
		Offset:     s.Offset,
		IsRawQuery: s.IsRawQuery,
	}
	for _, f := range s.Fields {
		clone.Fields = append(clone.Fields, &Field{Expr: CloneExpr(f.Expr), Alias: f.Alias})
	}
	for _, d := range s.Dimensions {
		clone.Dimensions = append(clone.Dimensions, &Dimension{Expr: CloneExpr(d.Expr)})
	}
	for _, f := range s.SortFields {
		clone.SortFields = append(clone.SortFields, &SortField{Name: f.Name, Ascending: f.Ascending})
	}
	return clone
}

// String returns a string representation of the select statement.
func (s *SelectStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SELECT ")
	_, _ = buf.WriteString(s.Fields.String())

	if len(s.Sources) > 0 {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute the SelectStatement.
func (s *SelectStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: ReadPrivilege}}
}

// OnlyTimeDimensions returns true if the statement has a where clause with only time constraints
func (s *SelectStatement) OnlyTimeDimensions() bool {
	return s.walkForTime(s.Condition)
}

// walkForTime is called by the OnlyTimeDimensions method to walk the where clause to determine if
// the only things specified are based on time
func (s *SelectStatement) walkForTime(node Node) bool {
	switch n := node.(type) {
	case *BinaryExpr:
		if n.Op == AND || n.Op == OR {
			return s.walkForTime(n.LHS) && s.walkForTime(n.RHS)
		}
		if ref, ok := n.LHS.(*VarRef); ok && strings.ToLower(ref.Val) == "time" {
			return true
		}
		return false
	case *ParenExpr:
		// walk down the tree
		return s.walkForTime(n.Expr)
	default:
		return false
	}
}

// HasWildcard returns whether or not the select statement has at least 1 wildcard
func (s *SelectStatement) HasWildcard() bool {
	for _, f := range s.Fields {
		_, ok := f.Expr.(*Wildcard)
		if ok {
			return true
		}
	}

	for _, d := range s.Dimensions {
		_, ok := d.Expr.(*Wildcard)
		if ok {
			return true
		}
	}

	return false
}

// hasTimeDimensions returns whether or not the select statement has at least 1
// where condition with time as the condition
func (s *SelectStatement) hasTimeDimensions(node Node) bool {
	switch n := node.(type) {
	case *BinaryExpr:
		if n.Op == AND || n.Op == OR {
			return s.hasTimeDimensions(n.LHS) || s.hasTimeDimensions(n.RHS)
		}
		if ref, ok := n.LHS.(*VarRef); ok && strings.ToLower(ref.Val) == "time" {
			return true
		}
		return false
	case *ParenExpr:
		// walk down the tree
		return s.hasTimeDimensions(n.Expr)
	default:
		return false
	}
}

// SetTimeRange sets the start and end time of the select statement to [start, end). i.e. start inclusive, end exclusive.
// This is used commonly for continuous queries so the start and end are in buckets.
func (s *SelectStatement) SetTimeRange(start, end time.Time) error {
	cond := fmt.Sprintf("time >= '%s' AND time < '%s'", start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))
	if s.Condition != nil {
		cond = fmt.Sprintf("%s AND %s", s.rewriteWithoutTimeDimensions(), cond)
	}

	expr, err := NewParser(strings.NewReader(cond)).ParseExpr()
	if err != nil {
		return err
	}

	// fold out any previously replaced time dimensios and set the condition
	s.Condition = Reduce(expr, nil)

	return nil
}

// rewriteWithoutTimeDimensions will remove any WHERE time... clauses from the select statement
// This is necessary when setting an explicit time range to override any that previously existed.
func (s *SelectStatement) rewriteWithoutTimeDimensions() string {
	n := RewriteFunc(s.Condition, func(n Node) Node {
		switch n := n.(type) {
		case *BinaryExpr:
			if n.LHS.String() == "time" {
				return &BooleanLiteral{Val: true}
			}
			return n
		case *Call:
			return &BooleanLiteral{Val: true}
		default:
			return n
		}
	})

	return n.String()
}

// NamesInWhere returns the field and tag names (idents) referenced in the where clause
func (s *SelectStatement) NamesInWhere() []string {
	var a []string
	if s.Condition != nil {
		a = walkNames(s.Condition)
	}
	return a
}

// NamesInSelect returns the field and tag names (idents) in the select clause
func (s *SelectStatement) NamesInSelect() []string {
	var a []string

	for _, f := range s.Fields {
		a = append(a, walkNames(f.Expr)...)
	}

	return a
}

// FunctionCalls returns the Call objects from the query
func (s *SelectStatement) FunctionCalls() []*Call {
	var a []*Call
	for _, f := range s.Fields {
		a = append(a, walkFunctionCalls(f.Expr)...)
	}
	return a
}

// DeleteStatement represents a command for removing data from the database.
type DeleteStatement struct {
	// Data source that values are removed from.
	Source Source

	// An expression evaluated on data point.
	Condition Expr
}

// String returns a string representation of the delete statement.
func (s *DeleteStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DELETE ")
	_, _ = buf.WriteString(s.Source.String())
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	return s.String()
}

// RequiredPrivileges returns the privilege required to execute a DeleteStatement.
func (s *DeleteStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: WritePrivilege}}
}

// ShowOrganizationsStatement represents a command for listing organizations.
type ShowOrganizationsStatement struct{}

// String returns a string representation of the list continuous queries statement.
func (s *ShowOrganizationsStatement) String() string { return "SHOW ORGANIZATIONS" }

// RequiredPrivileges returns the privilege required to execute a ShowOrganizationsStatement.
func (s *ShowOrganizationsStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: ReadPrivilege}}
}

// DropContinuousQueryStatement represents a command for removing a organization.
type DropOrganizationStatement struct {
	Name     string
	Database string
}

// String returns a string representation of the statement.
func (s *DropOrganizationStatement) String() string {
	return fmt.Sprintf("DROP ORGANIZATION %s", s.Name)
}

// RequiredPrivileges returns the privilege(s) required to execute a DropOrganizationStatement
func (s *DropOrganizationStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: WritePrivilege}}
}

// ShowConversationsStatement represents a command for listing conversations in the organization.
type ShowConversationsStatement struct {
	// Namespaces(s) the conversations are listed for.
	Sources Sources

	// An expression evaluated on a conversation name or tag.
	Condition Expr

	// Fields to sort results by
	SortFields SortFields

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int
}

// String returns a string representation of the list series statement.
func (s *ShowConversationsStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW CONVERSATIONS")

	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a ShowConversationsStatement.
func (s *ShowConversationsStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: ReadPrivilege}}
}

// DropConversationStatement represents a command for removing a conversation from the database.
type DropConversationStatement struct {
	Name string
}

// String returns a string representation of the drop conversation statement.
func (s *DropConversationStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP CONVERSATION ")
	_, _ = buf.WriteString(s.Name)
	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a DropSeriesStatement.
func (s DropConversationStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: WritePrivilege}}
}

// ShowOrganizationMembersStatement represents a command for listing user privileges.
type ShowOrganizationMembersStatement struct {
	// Name of the user to display privileges.
	Name string

	Database string

	// Data source that fields are extracted from (optional)
	Sources Sources

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int
}

// String returns a string representation of the show members for organization.
func (s *ShowOrganizationMembersStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW ORGANIZATION MEMBERS FOR ")
	_, _ = buf.WriteString(s.Name)

	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a ShowOrganizationMembersStatement
func (s *ShowOrganizationMembersStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: ReadPrivilege}}
}

// ShowGrantsForUserStatement represents a command for listing user privileges.
type ShowGrantsForUserStatement struct {
	// Name of the user to display privileges.
	Name string
}

// String returns a string representation of the show grants for user.
func (s *ShowGrantsForUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW GRANTS FOR ")
	_, _ = buf.WriteString(s.Name)

	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a ShowGrantsForUserStatement
func (s *ShowGrantsForUserStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// ShowDevicesForUserStatement represents a command for listing user privileges.
type ShowDevicesForUserStatement struct {
	// Name of the user to display privileges.
	Name string
}

// String returns a string representation of the show devices for user.
func (s *ShowDevicesForUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW DEVICES FOR ")
	_, _ = buf.WriteString(s.Name)

	return buf.String()
}

// RequiredPrivileges returns the privilege required to execute a ShowDevicesForUserStatement
func (s *ShowDevicesForUserStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// ShowServersStatement represents a command for listing all servers.
type ShowServersStatement struct{}

// String returns a string representation of the show servers command.
func (s *ShowServersStatement) String() string { return "SHOW SERVERS" }

// RequiredPrivileges returns the privilege required to execute a ShowServersStatement
func (s *ShowServersStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// ShowDatabasesStatement represents a command for listing all databases in the cluster.
type ShowDatabasesStatement struct{}

// String returns a string representation of the list databases command.
func (s *ShowDatabasesStatement) String() string { return "SHOW DATABASES" }

// RequiredPrivileges returns the privilege required to execute a ShowDatabasesStatement
func (s *ShowDatabasesStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// ShowRetentionPoliciesStatement represents a command for listing retention policies.
type ShowRetentionPoliciesStatement struct {
	// Name of the database to list policies for.
	Database string
}

// String returns a string representation of a ShowRetentionPoliciesStatement.
func (s *ShowRetentionPoliciesStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW RETENTION POLICIES ")
	_, _ = buf.WriteString(s.Database)
	return buf.String()
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowRetentionPoliciesStatement
func (s *ShowRetentionPoliciesStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: ReadPrivilege}}
}

// ShowRetentionPoliciesStatement represents a command for displaying stats for a given server.
type ShowStatsStatement struct {
	// Hostname or IP of the server for stats.
	Host string
}

// String returns a string representation of a ShowStatsStatement.
func (s *ShowStatsStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW STATS ")
	if s.Host != "" {
		_, _ = buf.WriteString(s.Host)
	}
	return buf.String()
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowStatsStatement
func (s *ShowStatsStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// ShowDiagnosticsStatement represents a command for show node diagnostics.
type ShowDiagnosticsStatement struct{}

// String returns a string representation of the ShowDiagnosticsStatement.
func (s *ShowDiagnosticsStatement) String() string { return "SHOW DIAGNOSTICS" }

// RequiredPrivileges returns the privilege required to execute a ShowDiagnosticsStatement
func (s *ShowDiagnosticsStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

// ShowUsersStatement represents a command for listing users.
type ShowUsersStatement struct{}

// String returns a string representation of the ShowUsersStatement.
func (s *ShowUsersStatement) String() string {
	return "SHOW USERS"
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowUsersStatement
func (s *ShowUsersStatement) RequiredPrivileges() ExecutionPrivileges {
	return ExecutionPrivileges{{Name: "", Privilege: AllPrivileges}}
}

func cloneSources(sources Sources) Sources {
	clone := make(Sources, 0, len(sources))
	for _, s := range sources {
		clone = append(clone, cloneSource(s))
	}
	return clone
}

func cloneSource(s Source) Source {
	if s == nil {
		return nil
	}

	switch s := s.(type) {
	case *Conversation:
		m := &Conversation{Database: s.Database, RetentionPolicy: s.RetentionPolicy, Name: s.Name}
		if s.Regex != nil {
			m.Regex = &RegexLiteral{Val: regexp.MustCompile(s.Regex.Val.String())}
		}
		return m
	default:
		panic("unreachable")
	}
}

// walkNames will walk the Expr and return the database fields
func walkNames(exp Expr) []string {
	switch expr := exp.(type) {
	case *VarRef:
		return []string{expr.Val}
	case *Call:
		if len(expr.Args) == 0 {
			return nil
		}
		lit, ok := expr.Args[0].(*VarRef)
		if !ok {
			return nil
		}

		return []string{lit.Val}
	case *BinaryExpr:
		var ret []string
		ret = append(ret, walkNames(expr.LHS)...)
		ret = append(ret, walkNames(expr.RHS)...)
		return ret
	case *ParenExpr:
		return walkNames(expr.Expr)
	}

	return nil
}

// walkFunctionCalls walks the Field of a query for any function calls made
func walkFunctionCalls(exp Expr) []*Call {
	switch expr := exp.(type) {
	case *VarRef:
		return nil
	case *Call:
		return []*Call{expr}
	case *BinaryExpr:
		var ret []*Call
		ret = append(ret, walkFunctionCalls(expr.LHS)...)
		ret = append(ret, walkFunctionCalls(expr.RHS)...)
		return ret
	case *ParenExpr:
		return walkFunctionCalls(expr.Expr)
	}

	return nil
}
