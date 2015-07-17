package messageql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	// DateFormat represents the format for date literals.
	DateFormat = "2006-01-02"

	// DateTimeFormat represents the format for date time literals.
	DateTimeFormat = "2006-01-02 15:04:05.999999"
)

// Parser represents an MessageQL parser.
type Parser struct {
	s *bufScanner
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: newBufScanner(r)}
}

// ParseQuery parses a query string and returns its AST representation.
func ParseQuery(s string) (*Query, error) { return NewParser(strings.NewReader(s)).ParseQuery() }

// ParseStatement parses a statement string and returns its AST representation.
func ParseStatement(s string) (Statement, error) {
	return NewParser(strings.NewReader(s)).ParseStatement()
}

// MustParseStatement parses a statement string and returns its AST. Panic on error.
func MustParseStatement(s string) Statement {
	stmt, err := ParseStatement(s)
	if err != nil {
		panic(err.Error())
	}
	return stmt
}

// ParseExpr parses an expression string and returns its AST representation.
func ParseExpr(s string) (Expr, error) { return NewParser(strings.NewReader(s)).ParseExpr() }

// ParseQuery parses an MessageQL string and returns a Query AST object.
func (p *Parser) ParseQuery() (*Query, error) {
	var statements Statements
	var semi bool

	for {
		if tok, _, _ := p.scanIgnoreWhitespace(); tok == EOF {
			return &Query{Statements: statements}, nil
		} else if !semi && tok == SEMICOLON {
			semi = true
		} else {
			p.unscan()
			s, err := p.ParseStatement()
			if err != nil {
				return nil, err
			}
			statements = append(statements, s)
			semi = false
		}
	}
}

// ParseStatement parses an MessageQL string and returns a Statement AST object.
func (p *Parser) ParseStatement() (Statement, error) {
	// Inspect the first token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case SELECT:
		return p.parseSelectStatement(targetNotRequired)
	case DELETE:
		return p.parseDeleteStatement()
	case SHOW:
		return p.parseShowStatement()
	case CREATE:
		return p.parseCreateStatement()
	case DROP:
		return p.parseDropStatement()
	case GRANT:
		return p.parseGrantStatement()
	case REVOKE:
		return p.parseRevokeStatement()
	case ALTER:
		return p.parseAlterStatement()
	case SET:
		return p.parseSetPasswordUserStatement()
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"SELECT", "DELETE", "SHOW", "CREATE", "DROP", "GRANT", "REVOKE", "ALTER", "SET"}, pos)
	}
}

// parseShowStatement parses a string and returns a list statement.
// This function assumes the SHOW token has already been consumed.
func (p *Parser) parseShowStatement() (Statement, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case ORGANIZATION:
		tok, pos, lit := p.scanIgnoreWhitespace()
		if tok == MEMBERS {
			return p.parseShowOrganizationMembersStatement()
		}
		return nil, newParseError(tokstr(tok, lit), []string{"MEMBERS"}, pos)
	case ORGANIZATIONS:
		return p.parseShowOrganizationsStatement()
	case CONVERSATIONS:
		return p.parseShowConversationsStatement()
	case DEVICES:
		return p.parseShowDevicesForUserStatement()
	case GRANTS:
		return p.parseGrantsForUserStatement()
	case DATABASES:
		return p.parseShowDatabasesStatement()
	case SERVERS:
		return p.parseShowServersStatement()
	case RETENTION:
		tok, pos, lit := p.scanIgnoreWhitespace()
		if tok == POLICIES {
			return p.parseShowRetentionPoliciesStatement()
		}
		return nil, newParseError(tokstr(tok, lit), []string{"POLICIES"}, pos)
	case STATS:
		return p.parseShowStatsStatement()
	case DIAGNOSTICS:
		return p.parseShowDiagnosticsStatement()
	case USERS:
		return p.parseShowUsersStatement()
	}

	return nil, newParseError(tokstr(tok, lit), []string{"CONVERSATIONS", "ORGANIZATION", "ORGANIZATIONS", "DATABASES", "FIELD", "GRANTS", "RETENTION", "SERVERS", "TAG", "USERS"}, pos)
}

// parseCreateStatement parses a string and returns a create statement.
// This function assumes the CREATE token has already been consumed.
func (p *Parser) parseCreateStatement() (Statement, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok == DATABASE {
		return p.parseCreateDatabaseStatement()
	} else if tok == USER {
		return p.parseCreateUserStatement()
	} else if tok == RETENTION {
		tok, pos, lit = p.scanIgnoreWhitespace()
		if tok != POLICY {
			return nil, newParseError(tokstr(tok, lit), []string{"POLICY"}, pos)
		}
		return p.parseCreateRetentionPolicyStatement()
	}

	return nil, newParseError(tokstr(tok, lit), []string{"DATABASE", "USER", "RETENTION"}, pos)
}

// parseDropStatement parses a string and returns a drop statement.
// This function assumes the DROP token has already been consumed.
func (p *Parser) parseDropStatement() (Statement, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok == ORGANIZATION {
		return p.parseDropOrganizationStatement()
	} else if tok == CONVERSATION {
		return p.parseDropConversationStatement()
	} else if tok == DATABASE {
		return p.parseDropDatabaseStatement()
	} else if tok == RETENTION {
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok != POLICY {
			return nil, newParseError(tokstr(tok, lit), []string{"POLICY"}, pos)
		}
		return p.parseDropRetentionPolicyStatement()
	} else if tok == USER {
		return p.parseDropUserStatement()
	}

	return nil, newParseError(tokstr(tok, lit), []string{"ORGANIZATION", "CONVERSATION"}, pos)
}

// parseAlterStatement parses a string and returns an alter statement.
// This function assumes the ALTER token has already been consumed.
func (p *Parser) parseAlterStatement() (Statement, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok == RETENTION {
		if tok, pos, lit = p.scanIgnoreWhitespace(); tok != POLICY {
			return nil, newParseError(tokstr(tok, lit), []string{"POLICY"}, pos)
		}
		return p.parseAlterRetentionPolicyStatement()
	}

	return nil, newParseError(tokstr(tok, lit), []string{"RETENTION"}, pos)
}

// parseSetPasswordUserStatement parses a string and returns a set statement.
// This function assumes the SET token has already been consumed.
func (p *Parser) parseSetPasswordUserStatement() (*SetPasswordUserStatement, error) {
	stmt := &SetPasswordUserStatement{}

	// Consume the required PASSWORD FOR tokens.
	if err := p.parseTokens([]Token{PASSWORD, FOR}); err != nil {
		return nil, err
	}

	// Parse username
	ident, err := p.parseIdent()

	if err != nil {
		return nil, err
	}
	stmt.Name = ident

	// Consume the required = token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != EQ {
		return nil, newParseError(tokstr(tok, lit), []string{"="}, pos)
	}

	// Parse new user's password
	if ident, err = p.parseString(); err != nil {
		return nil, err
	}
	stmt.Password = ident

	return stmt, nil
}

// parseCreateRetentionPolicyStatement parses a string and returns a create retention policy statement.
// This function assumes the CREATE RETENTION POLICY tokens have already been consumed.
func (p *Parser) parseCreateRetentionPolicyStatement() (*CreateRetentionPolicyStatement, error) {
	stmt := &CreateRetentionPolicyStatement{}

	// Parse the retention policy name.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = ident

	// Consume the required ON token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != ON {
		return nil, newParseError(tokstr(tok, lit), []string{"ON"}, pos)
	}

	// Parse the database name.
	ident, err = p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Database = ident

	// Parse required DURATION token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != DURATION {
		return nil, newParseError(tokstr(tok, lit), []string{"DURATION"}, pos)
	}

	// Parse duration value
	d, err := p.parseDuration()
	if err != nil {
		return nil, err
	}
	stmt.Duration = d

	// Parse required REPLICATION token.
	if tok, pos, lit = p.scanIgnoreWhitespace(); tok != REPLICATION {
		return nil, newParseError(tokstr(tok, lit), []string{"REPLICATION"}, pos)
	}

	// Parse replication value.
	n, err := p.parseInt(1, math.MaxInt32)
	if err != nil {
		return nil, err
	}
	stmt.Replication = n

	// Parse optional DEFAULT token.
	if tok, pos, lit = p.scanIgnoreWhitespace(); tok == DEFAULT {
		stmt.Default = true
	} else {
		p.unscan()
	}

	return stmt, nil
}

// parseAlterRetentionPolicyStatement parses a string and returns an alter retention policy statement.
// This function assumes the ALTER RETENTION POLICY tokens have already been consumed.
func (p *Parser) parseAlterRetentionPolicyStatement() (*AlterRetentionPolicyStatement, error) {
	stmt := &AlterRetentionPolicyStatement{}

	// Parse the retention policy name.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok == DEFAULT {
		stmt.Name = "default"
	} else if tok == IDENT {
		stmt.Name = lit
	} else {
		return nil, newParseError(tokstr(tok, lit), []string{"identifier"}, pos)
	}

	// Consume the required ON token.
	if tok, pos, lit = p.scanIgnoreWhitespace(); tok != ON {
		return nil, newParseError(tokstr(tok, lit), []string{"ON"}, pos)
	}

	// Parse the database name.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Database = ident

	// Loop through option tokens (DURATION, REPLICATION, DEFAULT, etc.).
	maxNumOptions := 3
Loop:
	for i := 0; i < maxNumOptions; i++ {
		tok, pos, lit := p.scanIgnoreWhitespace()
		switch tok {
		case DURATION:
			d, err := p.parseDuration()
			if err != nil {
				return nil, err
			}
			stmt.Duration = &d
		case REPLICATION:
			n, err := p.parseInt(1, math.MaxInt32)
			if err != nil {
				return nil, err
			}
			stmt.Replication = &n
		case DEFAULT:
			stmt.Default = true
		default:
			if i < 1 {
				return nil, newParseError(tokstr(tok, lit), []string{"DURATION", "RETENTION", "DEFAULT"}, pos)
			}
			p.unscan()
			break Loop
		}
	}

	return stmt, nil
}

// parseInt parses a string and returns an integer literal.
func (p *Parser) parseInt(min, max int) (int, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Return an error if the number has a fractional part.
	if strings.Contains(lit, ".") {
		return 0, &ParseError{Message: "number must be an integer", Pos: pos}
	}

	// Convert string to int.
	n, err := strconv.Atoi(lit)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	} else if min > n || n > max {
		return 0, &ParseError{
			Message: fmt.Sprintf("invalid value %d: must be %d <= n <= %d", n, min, max),
			Pos:     pos,
		}
	}

	return n, nil
}

// parseUInt32 parses a string and returns a 32-bit unsigned integer literal.
func (p *Parser) parseUInt32() (uint32, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Convert string to unsigned 32-bit integer
	n, err := strconv.ParseUint(lit, 10, 32)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return uint32(n), nil
}

// parseUInt64 parses a string and returns a 64-bit unsigned integer literal.
func (p *Parser) parseUInt64() (uint64, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Convert string to unsigned 64-bit integer
	n, err := strconv.ParseUint(lit, 10, 64)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return uint64(n), nil
}

// parseDuration parses a string and returns a duration literal.
// This function assumes the DURATION token has already been consumed.
func (p *Parser) parseDuration() (time.Duration, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != DURATION_VAL && tok != INF {
		return 0, newParseError(tokstr(tok, lit), []string{"duration"}, pos)
	}

	if tok == INF {
		return 0, nil
	}

	d, err := ParseDuration(lit)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return d, nil
}

// parseIdent parses an identifier.
func (p *Parser) parseIdent() (string, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT {
		return "", newParseError(tokstr(tok, lit), []string{"identifier"}, pos)
	}
	return lit, nil
}

// parseIdentList parses a comma delimited list of identifiers.
func (p *Parser) parseIdentList() ([]string, error) {
	// Parse first (required) identifier.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			return idents, nil
		}

		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}
}

// parseSegmentedIdents parses a segmented identifiers.
// e.g.,  "db"."rp".measurement  or  "db"..measurement
func (p *Parser) parseSegmentedIdents() ([]string, error) {
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.scan(); tok != DOT {
			// No more segments so we're done.
			p.unscan()
			break
		}

		if ch := p.peekRune(); ch == '/' {
			// Next segment is a regex so we're done.
			break
		} else if ch == '.' {
			// Add an empty identifier.
			idents = append(idents, "")
			continue
		}

		// Parse the next identifier.
		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}

	if len(idents) > 3 {
		msg := fmt.Sprintf("too many segments in %s", QuoteIdent(idents...))
		return nil, &ParseError{Message: msg}
	}

	return idents, nil
}

// parserString parses a string.
func (p *Parser) parseString() (string, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != STRING {
		return "", newParseError(tokstr(tok, lit), []string{"string"}, pos)
	}
	return lit, nil
}

// parseRevokeStatement parses a string and returns a revoke statement.
// This function assumes the REVOKE token has already been consumed.
func (p *Parser) parseRevokeStatement() (*RevokeStatement, error) {
	stmt := &RevokeStatement{}

	// Parse the privilege to be revoked.
	priv, err := p.parsePrivilege()
	if err != nil {
		return nil, err
	}
	stmt.Privilege = priv

	// Parse ON clause.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok == ON {
		// Parse the name of the thing we're revoking a privilege to use.
		lit, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		stmt.On = lit

		tok, pos, lit = p.scanIgnoreWhitespace()
	} else if priv != AllPrivileges {
		// ALL PRIVILEGES is the only privilege allowed cluster-wide.
		// No ON clause means query is requesting cluster-wide.
		return nil, newParseError(tokstr(tok, lit), []string{"ON"}, pos)
	}

	// Check for required FROM token.
	if tok != FROM {
		return nil, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}

	// Parse the name of the user we're revoking the privilege from.
	lit, err = p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.User = lit

	return stmt, nil
}

// parseGrantStatement parses a string and returns a grant statement.
// This function assumes the GRANT token has already been consumed.
func (p *Parser) parseGrantStatement() (*GrantStatement, error) {
	stmt := &GrantStatement{}

	// Parse the privilege to be granted.
	priv, err := p.parsePrivilege()
	if err != nil {
		return nil, err
	}
	stmt.Privilege = priv

	// Parse ON clause.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok == ON {
		// Parse the name of the thing we're granting a privilege to use.
		lit, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		stmt.On = lit

		tok, pos, lit = p.scanIgnoreWhitespace()
	} else if priv != AllPrivileges {
		// ALL PRIVILEGES is the only privilege allowed cluster-wide.
		// No ON clause means query is requesting cluster-wide.
		return nil, newParseError(tokstr(tok, lit), []string{"ON"}, pos)
	}

	// Check for required TO token.
	if tok != TO {
		return nil, newParseError(tokstr(tok, lit), []string{"TO"}, pos)
	}

	// Parse the name of the user we're granting the privilege to.
	lit, err = p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.User = lit

	return stmt, nil
}

// parsePrivilege parses a string and returns a Privilege
func (p *Parser) parsePrivilege() (Privilege, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case READ:
		return ReadPrivilege, nil
	case WRITE:
		return WritePrivilege, nil
	case ALL:
		// Consume optional PRIVILEGES token
		tok, pos, lit = p.scanIgnoreWhitespace()
		if tok != PRIVILEGES {
			p.unscan()
		}
		return AllPrivileges, nil
	}
	return 0, newParseError(tokstr(tok, lit), []string{"READ", "WRITE", "ALL [PRIVILEGES]"}, pos)
}

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement(tr targetRequirement) (*SelectStatement, error) {
	stmt := &SelectStatement{}
	var err error

	// Parse fields: "FIELD+".
	if stmt.Fields, err = p.parseFields(); err != nil {
		return nil, err
	}

	// Parse source: "FROM".
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != FROM {
		return nil, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}
	if stmt.Sources, err = p.parseSources(); err != nil {
		return nil, err
	}

	// Parse condition: "WHERE EXPR".
	if stmt.Condition, err = p.parseCondition(); err != nil {
		return nil, err
	}

	// Parse sort: "ORDER BY FIELD+".
	if stmt.SortFields, err = p.parseOrderBy(); err != nil {
		return nil, err
	}

	// Parse limit: "LIMIT <n>".
	if stmt.Limit, err = p.parseOptionalTokenAndInt(LIMIT); err != nil {
		return nil, err
	}

	// Parse offset: "OFFSET <n>".
	if stmt.Offset, err = p.parseOptionalTokenAndInt(OFFSET); err != nil {
		return nil, err
	}

	// Set if the query is a raw data query or one with an aggregate
	stmt.IsRawQuery = true
	WalkFunc(stmt.Fields, func(n Node) {
		if _, ok := n.(*Call); ok {
			stmt.IsRawQuery = false
		}
	})

	return stmt, nil
}

// targetRequirement specifies whether or not a target clause is required.
type targetRequirement int

const (
	targetRequired targetRequirement = iota
	targetNotRequired
)

// parseDeleteStatement parses a delete string and returns a DeleteStatement.
// This function assumes the DELETE token has already been consumed.
func (p *Parser) parseDeleteStatement() (*DeleteStatement, error) {
	stmt := &DeleteStatement{}

	// Parse source
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != FROM {
		return nil, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}
	source, err := p.parseSource()
	if err != nil {
		return nil, err
	}
	stmt.Source = source

	// Parse condition: "WHERE EXPR".
	condition, err := p.parseCondition()
	if err != nil {
		return nil, err
	}
	stmt.Condition = condition

	return stmt, nil
}

// parseShowOrganizationMembersStatement parses a string and returns a ShowOrganizationMembersStatement.
// This functions assumes the "SHOW ORGANIZATION MEMBERS" tokens have already been consumed.
func (p *Parser) parseShowOrganizationMembersStatement() (*ShowOrganizationMembersStatement, error) {
	stmt := &ShowOrganizationMembersStatement{}
	var err error

	// Parse optional FROM.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok == FROM {
		if stmt.Sources, err = p.parseSources(); err != nil {
			return nil, err
		}
	} else {
		p.unscan()
	}

	// Parse limit: "LIMIT <n>".
	if stmt.Limit, err = p.parseOptionalTokenAndInt(LIMIT); err != nil {
		return nil, err
	}

	// Parse offset: "OFFSET <n>".
	if stmt.Offset, err = p.parseOptionalTokenAndInt(OFFSET); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseShowOrganizationsStatement parses a string and returns a ShowOrganizationsStatement.
// This function assumes the "SHOW ORGANIZATIONS" tokens have already been consumed.
func (p *Parser) parseShowOrganizationsStatement() (*ShowOrganizationsStatement, error) {
	stmt := &ShowOrganizationsStatement{}
	return stmt, nil
}

// parseShowConversationsStatement parses a string and returns a ShowConversationsStatement.
// This function assumes the "SHOW CONVERSATIONS" tokens have already been consumed.
func (p *Parser) parseShowConversationsStatement() (*ShowConversationsStatement, error) {
	stmt := &ShowConversationsStatement{}
	var err error

	// Parse condition: "WHERE EXPR".
	if stmt.Condition, err = p.parseCondition(); err != nil {
		return nil, err
	}

	// Parse sort: "ORDER BY FIELD+".
	if stmt.SortFields, err = p.parseOrderBy(); err != nil {
		return nil, err
	}

	// Parse limit: "LIMIT <n>".
	if stmt.Limit, err = p.parseOptionalTokenAndInt(LIMIT); err != nil {
		return nil, err
	}

	// Parse offset: "OFFSET <n>".
	if stmt.Offset, err = p.parseOptionalTokenAndInt(OFFSET); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseShowRetentionPoliciesStatement parses a string and returns a ShowRetentionPoliciesStatement.
// This function assumes the "SHOW RETENTION POLICIES" tokens have been consumed.
func (p *Parser) parseShowRetentionPoliciesStatement() (*ShowRetentionPoliciesStatement, error) {
	stmt := &ShowRetentionPoliciesStatement{}

	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Database = ident

	return stmt, nil
}

// parseShowUsersStatement parses a string and returns a ShowUsersStatement.
// This function assumes the "SHOW USERS" tokens have been consumed.
func (p *Parser) parseShowUsersStatement() (*ShowUsersStatement, error) {
	return &ShowUsersStatement{}, nil
}

// parseDropOrganizationStatement parses a string and returns a DropOrganizationStatement.
// This function assumes the "DROP ORGANIZATION" tokens have already been consumed.
func (p *Parser) parseDropOrganizationStatement() (*DropOrganizationStatement, error) {
	stmt := &DropOrganizationStatement{}

	// Parse the name of the measurement to be dropped.
	lit, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = lit

	return stmt, nil
}

// parseDropConversationStatemen parses a string and returns a DropConversationStatement.
// This function assumes the "DROP CONVERSATION" tokens have already been consumed.
func (p *Parser) parseDropConversationStatement() (*DropConversationStatement, error) {
	stmt := &DropConversationStatement{}
	var err error

	// Parse the name of the database to be dropped.
	lit, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = lit

	return stmt, nil
}

// parseShowServersStatement parses a string and returns a ShowServersStatement.
// This function assumes the "SHOW SERVERS" tokens have already been consumed.
func (p *Parser) parseShowServersStatement() (*ShowServersStatement, error) {
	stmt := &ShowServersStatement{}
	return stmt, nil
}

// parseGrantsForUserStatement parses a string and returns a ShowGrantsForUserStatement.
// This function assumes the "SHOW GRANTS" tokens have already been consumed.
func (p *Parser) parseGrantsForUserStatement() (*ShowGrantsForUserStatement, error) {
	stmt := &ShowGrantsForUserStatement{}

	// Expect a "FOR" token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != FOR {
		return nil, newParseError(tokstr(tok, lit), []string{"FOR"}, pos)
	}

	// Parse the name of the user to be displayed.
	lit, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = lit

	return stmt, nil
}

// parseShowDevicesForUserStatement parses a string and returns a ShowDevicesForUserStatement.
// This function assumes the "SHOW GRANTS" tokens have already been consumed.
func (p *Parser) parseShowDevicesForUserStatement() (*ShowDevicesForUserStatement, error) {
	stmt := &ShowDevicesForUserStatement{}

	// Expect a "FOR" token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != FOR {
		return nil, newParseError(tokstr(tok, lit), []string{"FOR"}, pos)
	}

	// Parse the name of the recipient to be displayed.
	lit, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = lit

	return stmt, nil
}

// parseShowDatabasesStatement parses a string and returns a ShowDatabasesStatement.
// This function assumes the "SHOW DATABASE" tokens have already been consumed.
func (p *Parser) parseShowDatabasesStatement() (*ShowDatabasesStatement, error) {
	stmt := &ShowDatabasesStatement{}
	return stmt, nil
}

// parseCreateDatabaseStatement parses a string and returns a CreateDatabaseStatement.
// This function assumes the "CREATE DATABASE" tokens have already been consumed.
func (p *Parser) parseCreateDatabaseStatement() (*CreateDatabaseStatement, error) {
	stmt := &CreateDatabaseStatement{}

	// Parse the name of the database to be created.
	lit, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = lit

	return stmt, nil
}

// parseDropDatabaseStatement parses a string and returns a DropDatabaseStatement.
// This function assumes the DROP DATABASE tokens have already been consumed.
func (p *Parser) parseDropDatabaseStatement() (*DropDatabaseStatement, error) {
	stmt := &DropDatabaseStatement{}

	// Parse the name of the database to be dropped.
	lit, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = lit

	return stmt, nil
}

// parseDropRetentionPolicyStatement parses a string and returns a DropRetentionPolicyStatement.
// This function assumes the DROP RETENTION POLICY tokens have been consumed.
func (p *Parser) parseDropRetentionPolicyStatement() (*DropRetentionPolicyStatement, error) {
	stmt := &DropRetentionPolicyStatement{}

	// Parse the policy name.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = ident

	// Consume the required ON token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != ON {
		return nil, newParseError(tokstr(tok, lit), []string{"ON"}, pos)
	}

	// Parse the database name.
	if stmt.Database, err = p.parseIdent(); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseCreateUserStatement parses a string and returns a CreateUserStatement.
// This function assumes the "CREATE USER" tokens have already been consumed.
func (p *Parser) parseCreateUserStatement() (*CreateUserStatement, error) {
	stmt := &CreateUserStatement{}

	// Parse name of the user to be created.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = ident

	// Consume "WITH PASSWORD" tokens
	if err := p.parseTokens([]Token{WITH, PASSWORD}); err != nil {
		return nil, err
	}

	// Parse new user's password
	if ident, err = p.parseString(); err != nil {
		return nil, err
	}
	stmt.Password = ident

	// Check for option WITH clause.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != WITH {
		p.unscan()
		return stmt, nil
	}

	// We only allow granting of "ALL PRIVILEGES" during CREATE USER.
	// All other privileges must be granted using a GRANT statement.
	if err := p.parseTokens([]Token{ALL, PRIVILEGES}); err != nil {
		return nil, err
	}
	stmt.Privilege = NewPrivilege(AllPrivileges)

	return stmt, nil
}

// parseDropUserStatement parses a string and returns a DropUserStatement.
// This function assumes the DROP USER tokens have already been consumed.
func (p *Parser) parseDropUserStatement() (*DropUserStatement, error) {
	stmt := &DropUserStatement{}

	// Parse the name of the user to be dropped.
	lit, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = lit

	return stmt, nil
}

// parseRetentionPolicy parses a string and returns a retention policy name.
// This function assumes the "WITH" token has already been consumed.
func (p *Parser) parseRetentionPolicy() (name string, dfault bool, err error) {
	// Check for optional DEFAULT token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok == DEFAULT {
		dfault = true
		tok, pos, lit = p.scanIgnoreWhitespace()
	}

	// Check for required RETENTION token.
	if tok != RETENTION {
		err = newParseError(tokstr(tok, lit), []string{"RETENTION"}, pos)
		return
	}

	// Check of required POLICY token.
	if tok, pos, lit = p.scanIgnoreWhitespace(); tok != POLICY {
		err = newParseError(tokstr(tok, lit), []string{"POLICY"}, pos)
		return
	}

	// Parse retention policy name.
	name, err = p.parseIdent()
	if err != nil {
		return
	}

	return
}

// parseShowStatsStatement parses a string and returns a ShowStatsStatement.
// This function assumes the "SHOW STATS" tokens have already been consumed.
func (p *Parser) parseShowStatsStatement() (*ShowStatsStatement, error) {
	stmt := &ShowStatsStatement{}
	var err error

	if tok, _, _ := p.scanIgnoreWhitespace(); tok == ON {
		stmt.Host, err = p.parseString()
	} else {
		p.unscan()
	}

	return stmt, err
}

// parseShowDiagnostics parses a string and returns a ShowDiagnosticsStatement.
func (p *Parser) parseShowDiagnosticsStatement() (*ShowDiagnosticsStatement, error) {
	stmt := &ShowDiagnosticsStatement{}
	return stmt, nil
}

// parseFields parses a list of one or more fields.
func (p *Parser) parseFields() (Fields, error) {
	var fields Fields

	// Check for "*" (i.e., "all fields")
	if tok, _, _ := p.scanIgnoreWhitespace(); tok == MUL {
		fields = append(fields, &Field{&Wildcard{}, ""})
		return fields, nil
	}
	p.unscan()

	for {
		// Parse the field.
		f, err := p.parseField()
		if err != nil {
			return nil, err
		}

		// Add new field.
		fields = append(fields, f)

		// If there's not a comma next then stop parsing fields.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}
	return fields, nil
}

// parseField parses a single field.
func (p *Parser) parseField() (*Field, error) {
	f := &Field{}

	// Parse the expression first.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	f.Expr = expr

	// Parse the alias if the current and next tokens are "WS AS".
	alias, err := p.parseAlias()
	if err != nil {
		return nil, err
	}
	f.Alias = alias

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return f, nil
}

// parseAlias parses the "AS (IDENT|STRING)" alias for fields and dimensions.
func (p *Parser) parseAlias() (string, error) {
	// Check if the next token is "AS". If not, then unscan and exit.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != AS {
		p.unscan()
		return "", nil
	}

	// Then we should have the alias identifier.
	lit, err := p.parseIdent()
	if err != nil {
		return "", err
	}
	return lit, nil
}

// parseSources parses a comma delimited list of sources.
func (p *Parser) parseSources() (Sources, error) {
	var sources Sources

	for {
		s, err := p.parseSource()
		if err != nil {
			return nil, err
		}
		sources = append(sources, s)

		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			break
		}
	}

	return sources, nil
}

// peekRune returns the next rune that would be read by the scanner.
func (p *Parser) peekRune() rune {
	r, _, _ := p.s.s.r.ReadRune()
	if r != eof {
		_ = p.s.s.r.UnreadRune()
	}

	return r
}

func (p *Parser) parseSource() (Source, error) {
	c := &Conversation{}

	// Attempt to parse a regex.
	re, err := p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		c.Regex = re
		// Regex is always last so we're done.
		return c, nil
	}

	// Didn't find a regex so parse segmented identifiers.
	idents, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}

	// If we already have the max allowed idents, we're done.
	if len(idents) == 3 {
		c.Database, c.RetentionPolicy, c.Name = idents[0], idents[1], idents[2]
		return c, nil
	}
	// Check again for regex.
	re, err = p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		c.Regex = re
	}

	// Assign identifiers to their proper locations.
	switch len(idents) {
	case 1:
		if re != nil {
			c.RetentionPolicy = idents[0]
		} else {
			c.Name = idents[0]
		}
	case 2:
		if re != nil {
			c.Database, c.RetentionPolicy = idents[0], idents[1]
		} else {
			c.RetentionPolicy, c.Name = idents[0], idents[1]
		}
	}

	return c, nil
}

// parseCondition parses the "WHERE" clause of the query, if it exists.
func (p *Parser) parseCondition() (Expr, error) {
	// Check if the WHERE token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != WHERE {
		p.unscan()
		return nil, nil
	}

	// Scan the identifier for the source.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	return expr, nil
}

// parseOptionalTokenAndInt parses the specified token followed
// by an int, if it exists.
func (p *Parser) parseOptionalTokenAndInt(t Token) (int, error) {
	// Check if the token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != t {
		p.unscan()
		return 0, nil
	}

	// Scan the number.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Return an error if the number has a fractional part.
	if strings.Contains(lit, ".") {
		msg := fmt.Sprintf("fractional parts not allowed in %s", t.String())
		return 0, &ParseError{Message: msg, Pos: pos}
	}

	// Parse number.
	n, _ := strconv.ParseInt(lit, 10, 64)

	if n < 0 {
		msg := fmt.Sprintf("%s must be >= 0", t.String())
		return 0, &ParseError{Message: msg, Pos: pos}
	}

	return int(n), nil
}

// parseOrderBy parses the "ORDER BY" clause of a query, if it exists.
func (p *Parser) parseOrderBy() (SortFields, error) {
	// Return nil result and nil error if no ORDER token at this position.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != ORDER {
		p.unscan()
		return nil, nil
	}

	// Parse the required BY token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != BY {
		return nil, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}

	// Parse the ORDER BY fields.
	fields, err := p.parseSortFields()
	if err != nil {
		return nil, err
	}

	return fields, nil
}

// parseSortFields parses all fields of and ORDER BY clause.
func (p *Parser) parseSortFields() (SortFields, error) {
	var fields SortFields

	// At least one field is required.
	field, err := p.parseSortField()
	if err != nil {
		return nil, err
	}
	fields = append(fields, field)

	// Parse additional fields.
	for {
		tok, _, _ := p.scanIgnoreWhitespace()

		if tok != COMMA {
			p.unscan()
			break
		}

		field, err := p.parseSortField()
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)
	}

	return fields, nil
}

// parseSortField parses one field of an ORDER BY clause.
func (p *Parser) parseSortField() (*SortField, error) {
	field := &SortField{}

	// Next token must be ASC, until other sort orders are supported.
	tok, _, _ := p.scanIgnoreWhitespace()
	if tok != ASC {
		return nil, errors.New("only ORDER BY ASC supported at this time")
	}

	field.Ascending = true
	return field, nil
}

// parseVarRef parses a reference to a measurement or field.
func (p *Parser) parseVarRef() (*VarRef, error) {
	// Parse the segments of the variable ref.
	segments, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}

	vr := &VarRef{Val: strings.Join(segments, ".")}

	return vr, nil
}

// ParseExpr parses an expression.
func (p *Parser) ParseExpr() (Expr, error) {
	var err error
	// Dummy root node.
	root := &BinaryExpr{}

	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	root.RHS, err = p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		// If the next token is NOT an operator then return the expression.
		op, _, _ := p.scanIgnoreWhitespace()
		if !op.isOperator() {
			p.unscan()
			return root.RHS, nil
		}

		// Otherwise parse the next expression.
		var rhs Expr
		if IsRegexOp(op) {
			// RHS of a regex operator must be a regular expression.
			p.consumeWhitespace()
			if rhs, err = p.parseRegex(); err != nil {
				return nil, err
			}
		} else {
			if rhs, err = p.parseUnaryExpr(); err != nil {
				return nil, err
			}
		}

		// Find the right spot in the tree to add the new expression by
		// descending the RHS of the expression tree until we reach the last
		// BinaryExpr or a BinaryExpr whose RHS has an operator with
		// precedence >= the operator being added.
		for node := root; ; {
			r, ok := node.RHS.(*BinaryExpr)
			if !ok || r.Op.Precedence() >= op.Precedence() {
				// Add the new expression here and break.
				node.RHS = &BinaryExpr{LHS: node.RHS, RHS: rhs, Op: op}
				break
			}
			node = r
		}
	}
}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr() (Expr, error) {
	// If the first token is a LPAREN then parse it as its own grouped expression.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok == LPAREN {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}

		// Expect an RPAREN at the end.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok != RPAREN {
			return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
		}

		return &ParenExpr{Expr: expr}, nil
	}
	p.unscan()

	// Read next token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case IDENT:
		// If the next immediate token is a left parentheses, parse as function call.
		// Otherwise parse as a variable reference.
		if tok0, _, _ := p.scan(); tok0 == LPAREN {
			return p.parseCall(lit)
		}

		p.unscan() // unscan the last token (wasn't an LPAREN)
		p.unscan() // unscan the IDENT token

		// Parse it as a VarRef.
		return p.parseVarRef()
	case STRING:
		// If literal looks like a date time then parse it as a time literal.
		if isDateTimeString(lit) {
			t, err := time.Parse(DateTimeFormat, lit)
			if err != nil {
				// try to parse it as an RFCNano time
				t, err := time.Parse(time.RFC3339Nano, lit)
				if err != nil {
					return nil, &ParseError{Message: "unable to parse datetime", Pos: pos}
				}
				return &TimeLiteral{Val: t}, nil
			}
			return &TimeLiteral{Val: t}, nil
		} else if isDateString(lit) {
			t, err := time.Parse(DateFormat, lit)
			if err != nil {
				return nil, &ParseError{Message: "unable to parse date", Pos: pos}
			}
			return &TimeLiteral{Val: t}, nil
		}
		return &StringLiteral{Val: lit}, nil
	case NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return &NumberLiteral{Val: v}, nil
	case TRUE, FALSE:
		return &BooleanLiteral{Val: (tok == TRUE)}, nil
	case DURATION_VAL:
		v, _ := ParseDuration(lit)
		return &DurationLiteral{Val: v}, nil
	case MUL:
		return &Wildcard{}, nil
	case REGEX:
		re, err := regexp.Compile(lit)
		if err != nil {
			return nil, &ParseError{Message: err.Error(), Pos: pos}
		}
		return &RegexLiteral{Val: re}, nil
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// parseRegex parses a regular expression.
func (p *Parser) parseRegex() (*RegexLiteral, error) {
	nextRune := p.peekRune()
	if isWhitespace(nextRune) {
		p.consumeWhitespace()
	}

	// If the next character is not a '/', then return nils.
	nextRune = p.peekRune()
	if nextRune != '/' {
		return nil, nil
	}

	tok, pos, lit := p.s.ScanRegex()

	if tok == BADESCAPE {
		msg := fmt.Sprintf("bad escape: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok == BADREGEX {
		msg := fmt.Sprintf("bad regex: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok != REGEX {
		return nil, newParseError(tokstr(tok, lit), []string{"regex"}, pos)
	}

	re, err := regexp.Compile(lit)
	if err != nil {
		return nil, &ParseError{Message: err.Error(), Pos: pos}
	}

	return &RegexLiteral{Val: re}, nil
}

// parseCall parses a function call.
// This function assumes the function name and LPAREN have been consumed.
func (p *Parser) parseCall(name string) (*Call, error) {
	name = strings.ToLower(name)
	// If there's a right paren then just return immediately.
	if tok, _, _ := p.scan(); tok == RPAREN {
		return &Call{Name: name}, nil
	}
	p.unscan()

	// Otherwise parse function call arguments.
	var args []Expr
	for {
		// Parse an expression argument.
		arg, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		// If there's not a comma next then stop parsing arguments.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}

	// There should be a right parentheses at the end.
	if tok, pos, lit := p.scan(); tok != RPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	return &Call{Name: name, Args: args}, nil
}

// scan returns the next token from the underlying scanner.
func (p *Parser) scan() (tok Token, pos Pos, lit string) { return p.s.Scan() }

// scanIgnoreWhitespace scans the next non-whitespace token.
func (p *Parser) scanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	tok, pos, lit = p.scan()
	if tok == WS {
		tok, pos, lit = p.scan()
	}
	return
}

// consumeWhitespace scans the next token if it's whitespace.
func (p *Parser) consumeWhitespace() {
	if tok, _, _ := p.scan(); tok != WS {
		p.unscan()
	}
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() { p.s.Unscan() }

// ParseDuration parses a time duration from a string.
func ParseDuration(s string) (time.Duration, error) {
	// Return an error if the string is blank.
	if len(s) == 0 {
		return 0, ErrInvalidDuration
	}

	// If there's only character then it must be a digit (in microseconds).
	if len(s) == 1 {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return time.Duration(n) * time.Microsecond, nil
		}
		return 0, ErrInvalidDuration
	}

	// Split string into individual runes.
	a := split(s)

	// Extract the unit of measure.
	// If the last character is a digit then parse the whole string as microseconds.
	// If the last two characters are "ms" the parse as milliseconds.
	// Otherwise just use the last character as the unit of measure.
	var num, uom string
	if isDigit(rune(a[len(a)-1])) {
		num, uom = s, "u"
	} else if len(s) > 2 && s[len(s)-2:] == "ms" {
		num, uom = string(a[:len(a)-2]), "ms"
	} else {
		num, uom = string(a[:len(a)-1]), string(a[len(a)-1:])
	}

	// Parse the numeric part.
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, ErrInvalidDuration
	}

	// Multiply by the unit of measure.
	switch uom {
	case "u", "µ":
		return time.Duration(n) * time.Microsecond, nil
	case "ms":
		return time.Duration(n) * time.Millisecond, nil
	case "s":
		return time.Duration(n) * time.Second, nil
	case "m":
		return time.Duration(n) * time.Minute, nil
	case "h":
		return time.Duration(n) * time.Hour, nil
	case "d":
		return time.Duration(n) * 24 * time.Hour, nil
	case "w":
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	default:
		return 0, ErrInvalidDuration
	}
}

// FormatDuration formats a duration to a string.
func FormatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	} else if d%(7*24*time.Hour) == 0 {
		return fmt.Sprintf("%dw", d/(7*24*time.Hour))
	} else if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	} else if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", d/time.Hour)
	} else if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", d/time.Minute)
	} else if d%time.Second == 0 {
		return fmt.Sprintf("%ds", d/time.Second)
	} else if d%time.Millisecond == 0 {
		return fmt.Sprintf("%dms", d/time.Millisecond)
	}
	return fmt.Sprintf("%d", d/time.Microsecond)
}

// parseTokens consumes an expected sequence of tokens.
func (p *Parser) parseTokens(toks []Token) error {
	for _, expected := range toks {
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok != expected {
			return newParseError(tokstr(tok, lit), []string{tokens[expected]}, pos)
		}
	}
	return nil
}

// QuoteString returns a quoted string.
func QuoteString(s string) string {
	return `'` + strings.NewReplacer("\n", `\n`, `\`, `\\`, `'`, `\'`).Replace(s) + `'`
}

// QuoteIdent returns a quoted identifier from multiple bare identifiers.
func QuoteIdent(segments ...string) string {
	r := strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)

	var buf bytes.Buffer
	for i, segment := range segments {
		needQuote := IdentNeedsQuotes(segment) ||
			((i < len(segments)-1) && segment != "") // not last segment && not ""

		if needQuote {
			_ = buf.WriteByte('"')
		}

		_, _ = buf.WriteString(r.Replace(segment))

		if needQuote {
			_ = buf.WriteByte('"')
		}

		if i < len(segments)-1 {
			_ = buf.WriteByte('.')
		}
	}
	return buf.String()
}

// IdentNeedsQuotes returns true if the ident string given would require quotes.
func IdentNeedsQuotes(ident string) bool {
	for i, r := range ident {
		if i == 0 && !isIdentFirstChar(r) {
			return true
		} else if i > 0 && !isIdentChar(r) {
			return true
		}
	}
	return false
}

// split splits a string into a slice of runes.
func split(s string) (a []rune) {
	for _, ch := range s {
		a = append(a, ch)
	}
	return
}

// isDateString returns true if the string looks like a date-only time literal.
func isDateString(s string) bool { return dateStringRegexp.MatchString(s) }

// isDateTimeString returns true if the string looks like a date+time time literal.
func isDateTimeString(s string) bool { return dateTimeStringRegexp.MatchString(s) }

var dateStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
var dateTimeStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}.+`)

// ErrInvalidDuration is returned when parsing a malformatted duration.
var ErrInvalidDuration = errors.New("invalid duration")

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d", e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}
