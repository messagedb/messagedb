package messageql

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DataType represents the primitive data types available in MessageQL.
type DataType int

const (
	// Unknown primitive data type.
	Unknown DataType = 0
	// Float means the data type is a float
	Float = 1
	// Integer means the data type is a integer
	Integer = 2
	// Boolean means the data type is a boolean.
	Boolean = 3
	// String means the data type is a string of text.
	String = 4
	// Time means the data type is a time.
	Time = 5
	// Duration means the data type is a duration of time.
	Duration = 6
)

// InspectDataType returns the data type of a given value.
func InspectDataType(v interface{}) DataType {
	switch v.(type) {
	case float64:
		return Float
	case int64, int32, int:
		return Integer
	case bool:
		return Boolean
	case string:
		return String
	case time.Time:
		return Time
	case time.Duration:
		return Duration
	default:
		return Unknown
	}
}

func (d DataType) String() string {
	switch d {
	case Float:
		return "float"
	case Integer:
		return "integer"
	case Boolean:
		return "boolean"
	case String:
		return "string"
	case Time:
		return "time"
	case Duration:
		return "duration"
	}
	return "unknown"
}

// Node represents a node in the InfluxDB abstract syntax tree.
type Node interface {
	node()
	String() string
}

func (*Query) node()     {}
func (Statements) node() {}

func (*AlterRetentionPolicyStatement) node() {}

func (*CreateDatabaseStatement) node()        {}
func (*CreateRetentionPolicyStatement) node() {}
func (*CreateUserStatement) node()            {}

func (*DeleteStatement) node() {}

func (*DropDatabaseStatement) node() {}

func (*DropRetentionPolicyStatement) node() {}

func (*DropOrganizationStatement) node() {}
func (*DropConversationStatement) node() {}
func (*DropUserStatement) node()         {}
func (*GrantStatement) node()            {}
func (*RevokeStatement) node()           {}
func (*SelectStatement) node()           {}
func (*SetPasswordUserStatement) node()  {}

func (*ShowGrantsForUserStatement) node()       {}
func (*ShowDevicesForUserStatement) node()      {}
func (*ShowServersStatement) node()             {}
func (*ShowDatabasesStatement) node()           {}
func (*ShowRetentionPoliciesStatement) node()   {}
func (*ShowConversationsStatement) node()       {}
func (*ShowOrganizationsStatement) node()       {}
func (*ShowOrganizationMembersStatement) node() {}
func (*ShowStatsStatement) node()               {}
func (*ShowDiagnosticsStatement) node()         {}
func (*ShowUsersStatement) node()               {}

func (*BinaryExpr) node()      {}
func (*BooleanLiteral) node()  {}
func (*Call) node()            {}
func (*Dimension) node()       {}
func (Dimensions) node()       {}
func (*DurationLiteral) node() {}
func (*Field) node()           {}
func (Fields) node()           {}
func (*Conversation) node()    {}
func (Conversations) node()    {}
func (*nilLiteral) node()      {}
func (*NumberLiteral) node()   {}
func (*ParenExpr) node()       {}
func (*RegexLiteral) node()    {}
func (*SortField) node()       {}
func (SortFields) node()       {}
func (Sources) node()          {}
func (*StringLiteral) node()   {}
func (*TimeLiteral) node()     {}
func (*VarRef) node()          {}
func (*Wildcard) node()        {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	expr()
}

func (*BinaryExpr) expr()      {}
func (*BooleanLiteral) expr()  {}
func (*Call) expr()            {}
func (*DurationLiteral) expr() {}
func (*nilLiteral) expr()      {}
func (*NumberLiteral) expr()   {}
func (*ParenExpr) expr()       {}
func (*RegexLiteral) expr()    {}
func (*StringLiteral) expr()   {}
func (*TimeLiteral) expr()     {}
func (*VarRef) expr()          {}
func (*Wildcard) expr()        {}

// Source represents a source of data for a statement.
type Source interface {
	Node
	source()
}

func (*Conversation) source() {}

// Sources represents a list of sources.
type Sources []Source

// String returns a string representation of a Sources array.
func (a Sources) String() string {
	var buf bytes.Buffer

	ubound := len(a) - 1
	for i, src := range a {
		_, _ = buf.WriteString(src.String())
		if i < ubound {
			_, _ = buf.WriteString(", ")
		}
	}

	return buf.String()
}

// SortField represents a field to sort results by.
type SortField struct {
	// Name of the field
	Name string

	// Sort order.
	Ascending bool
}

// String returns a string representation of a sort field
func (field *SortField) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(field.Name)
	_, _ = buf.WriteString(" ")
	_, _ = buf.WriteString(strconv.FormatBool(field.Ascending))
	return buf.String()
}

// SortFields represents an ordered list of ORDER BY fields
type SortFields []*SortField

// String returns a string representation of sort fields
func (a SortFields) String() string {
	fields := make([]string, 0, len(a))
	for _, field := range a {
		fields = append(fields, field.String())
	}
	return strings.Join(fields, ", ")
}

// Fields represents a list of fields.
type Fields []*Field

// String returns a string representation of the fields.
func (ff Fields) String() string {
	var str []string
	for _, f := range ff {
		str = append(str, f.String())
	}
	return strings.Join(str, ", ")
}

// Sort Interface for Fields
func (ff Fields) Len() int           { return len(ff) }
func (ff Fields) Less(i, j int) bool { return ff[i].Name() < ff[j].Name() }
func (ff Fields) Swap(i, j int)      { ff[i], ff[j] = ff[j], ff[i] }

// Field represents an expression retrieved from a select statement.
type Field struct {
	Expr  Expr
	Alias string
}

// Name returns the name of the field. Returns alias, if set.
// Otherwise uses the function name or variable name.
func (f *Field) Name() string {
	// Return alias, if set.
	if f.Alias != "" {
		return f.Alias
	}

	// Return the function name or variable name, if available.
	switch expr := f.Expr.(type) {
	case *Call:
		return expr.Name
	case *VarRef:
		return expr.Val
	}

	// Otherwise return a blank name.
	return ""
}

// String returns a string representation of the field.
func (f *Field) String() string {
	if f.Alias == "" {
		return f.Expr.String()
	}
	return fmt.Sprintf("%s AS %s", f.Expr.String(), f.Alias)
}

// Dimensions represents a list of dimensions.
type Dimensions []*Dimension

// String returns a string representation of the dimensions.
func (a Dimensions) String() string {
	var str []string
	for _, d := range a {
		str = append(str, d.String())
	}
	return strings.Join(str, ", ")
}

// Normalize returns the interval and tag dimensions separately.
// Returns 0 if no time interval is specified.
// Returns an error if multiple time dimensions exist or if non-VarRef dimensions are specified.
func (a Dimensions) Normalize() (time.Duration, []string, error) {
	var dur time.Duration
	var tags []string

	for _, dim := range a {
		switch expr := dim.Expr.(type) {
		case *Call:
			// Ensure the call is time() and it only has one duration argument.
			// If we already have a duration
			if expr.Name != "time" {
				return 0, nil, errors.New("only time() calls allowed in dimensions")
			} else if len(expr.Args) != 1 {
				return 0, nil, errors.New("time dimension expected one argument")
			} else if lit, ok := expr.Args[0].(*DurationLiteral); !ok {
				return 0, nil, errors.New("time dimension must have one duration argument")
			} else if dur != 0 {
				return 0, nil, errors.New("multiple time dimensions not allowed")
			} else {
				dur = lit.Val
			}

		case *VarRef:
			tags = append(tags, expr.Val)

		default:
			return 0, nil, errors.New("only time and tag dimensions allowed")
		}
	}

	return dur, tags, nil
}

// Dimension represents an expression that a select statement is grouped by.
type Dimension struct {
	Expr Expr
}

// String returns a string representation of the dimension.
func (d *Dimension) String() string { return d.Expr.String() }

// VarRef represents a reference to a variable.
type VarRef struct {
	Val string
}

// Call represents a function call.
type Call struct {
	Name string
	Args []Expr
}

// String returns a string representation of the call.
func (c *Call) String() string {
	// Join arguments.
	var str []string
	for _, arg := range c.Args {
		str = append(str, arg.String())
	}

	// Write function name and args.
	return fmt.Sprintf("%s(%s)", c.Name, strings.Join(str, ", "))
}

// Organizations represents a list of organizations
type Organizations []*Organization

// String returns a string representation of the organizations.
func (oo Organizations) String() string {
	var str []string
	for _, o := range oo {
		str = append(str, o.String())
	}
	return strings.Join(str, ", ")
}

// Organization represents a single organization used as a datasource.
type Organization struct {
	Database        string
	RetentionPolicy string
	Name            string
	Regex           *RegexLiteral
}

// String returns a string representation of the conversation.
func (o *Organization) String() string {
	var buf bytes.Buffer
	if o.Database != "" {
		_, _ = buf.WriteString(`"`)
		_, _ = buf.WriteString(o.Database)
		_, _ = buf.WriteString(`".`)
	}

	if o.RetentionPolicy != "" {
		_, _ = buf.WriteString(`"`)
		_, _ = buf.WriteString(o.RetentionPolicy)
		_, _ = buf.WriteString(`"`)
	}

	if o.Database != "" || o.RetentionPolicy != "" {
		_, _ = buf.WriteString(`.`)
	}

	if o.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(o.Name))
	} else if o.Regex != nil {
		_, _ = buf.WriteString(o.Regex.String())
	}

	return buf.String()
}

// Conversations represents a list of conversations.
type Conversations []*Conversation

// String returns a string representation of the conversations.
func (a Conversations) String() string {
	var str []string
	for _, m := range a {
		str = append(str, m.String())
	}
	return strings.Join(str, ", ")
}

// Conversation represents a single conversation used as a datasource.
type Conversation struct {
	Database        string
	RetentionPolicy string
	Name            string
	Regex           *RegexLiteral
}

// String returns a string representation of the conversation.
func (m *Conversation) String() string {
	var buf bytes.Buffer
	if m.Database != "" {
		_, _ = buf.WriteString(`"`)
		_, _ = buf.WriteString(m.Database)
		_, _ = buf.WriteString(`".`)
	}

	if m.RetentionPolicy != "" {
		_, _ = buf.WriteString(`"`)
		_, _ = buf.WriteString(m.RetentionPolicy)
		_, _ = buf.WriteString(`"`)
	}

	if m.Database != "" || m.RetentionPolicy != "" {
		_, _ = buf.WriteString(`.`)
	}

	if m.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(m.Name))
	} else if m.Regex != nil {
		_, _ = buf.WriteString(m.Regex.String())
	}

	return buf.String()
}

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  Token
	LHS Expr
	RHS Expr
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op.String(), e.RHS.String())
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string { return fmt.Sprintf("(%s)", e.Expr.String()) }

// Wildcard represents a wild card expression.
type Wildcard struct{}

// String returns a string representation of the wildcard.
func (e *Wildcard) String() string { return "*" }

// CloneExpr returns a deep copy of the expression.
func CloneExpr(expr Expr) Expr {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *BinaryExpr:
		return &BinaryExpr{Op: expr.Op, LHS: CloneExpr(expr.LHS), RHS: CloneExpr(expr.RHS)}
	case *BooleanLiteral:
		return &BooleanLiteral{Val: expr.Val}
	case *DurationLiteral:
		return &DurationLiteral{Val: expr.Val}
	case *NumberLiteral:
		return &NumberLiteral{Val: expr.Val}
	case *ParenExpr:
		return &ParenExpr{Expr: CloneExpr(expr.Expr)}
	case *RegexLiteral:
		return &RegexLiteral{Val: expr.Val}
	case *StringLiteral:
		return &StringLiteral{Val: expr.Val}
	case *TimeLiteral:
		return &TimeLiteral{Val: expr.Val}
	case *VarRef:
		return &VarRef{Val: expr.Val}
	case *Wildcard:
		return &Wildcard{}
	}
	panic("unreachable")
}

// TimeRange returns the minimum and maximum times specified by an expression.
// Returns zero times if there is no bound.
func TimeRange(expr Expr) (min, max time.Time) {
	WalkFunc(expr, func(n Node) {
		if n, ok := n.(*BinaryExpr); ok {
			// Extract literal expression & operator on LHS.
			// Check for "time" on the left-hand side first.
			// Otherwise check for for the right-hand side and flip the operator.
			value, op := timeExprValue(n.LHS, n.RHS), n.Op
			if value.IsZero() {
				if value = timeExprValue(n.RHS, n.LHS); value.IsZero() {
					return
				} else if op == LT {
					op = GT
				} else if op == LTE {
					op = GTE
				} else if op == GT {
					op = LT
				} else if op == GTE {
					op = LTE
				}
			}

			// Update the min/max depending on the operator.
			// The GT & LT update the value by +/- 1µs not make them "not equal".
			switch op {
			case GT:
				if min.IsZero() || value.After(min) {
					min = value.Add(time.Microsecond)
				}
			case GTE:
				if min.IsZero() || value.After(min) {
					min = value
				}
			case LT:
				if max.IsZero() || value.Before(max) {
					max = value.Add(-time.Microsecond)
				}
			case LTE:
				if max.IsZero() || value.Before(max) {
					max = value
				}
			case EQ:
				if min.IsZero() || value.After(min) {
					min = value
				}
				if max.IsZero() || value.Before(max) {
					max = value
				}
			}
		}
	})
	return
}

// Visitor can be called by Walk to traverse an AST hierarchy.
// The Visit() function is called once per node.
type Visitor interface {
	Visit(Node) Visitor
}

// Walk traverses a node hierarchy in depth-first order.
func Walk(v Visitor, node Node) {
	if node == nil {
		return
	}

	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *Dimension:
		Walk(v, n.Expr)

	case Dimensions:
		for _, c := range n {
			Walk(v, c)
		}

	case *Field:
		Walk(v, n.Expr)

	case Fields:
		for _, c := range n {
			Walk(v, c)
		}

	case *ParenExpr:
		Walk(v, n.Expr)

	case *Query:
		Walk(v, n.Statements)

	case *SelectStatement:
		Walk(v, n.Fields)
		Walk(v, n.Sources)
		Walk(v, n.Condition)
		Walk(v, n.SortFields)

	case *ShowConversationsStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case SortFields:
		for _, sf := range n {
			Walk(v, sf)
		}

	case Sources:
		for _, s := range n {
			Walk(v, s)
		}

	case Statements:
		for _, s := range n {
			Walk(v, s)
		}

	}
}

// WalkFunc traverses a node hierarchy in depth-first order.
func WalkFunc(node Node, fn func(Node)) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Node)

func (fn walkFuncVisitor) Visit(n Node) Visitor { fn(n); return fn }

// Rewriter can be called by Rewrite to replace nodes in the AST hierarchy.
// The Rewrite() function is called once per node.
type Rewriter interface {
	Rewrite(Node) Node
}

// Rewrite recursively invokes the rewriter to replace each node.
// Nodes are traversed depth-first and rewritten from leaf to root.
func Rewrite(r Rewriter, node Node) Node {
	switch n := node.(type) {
	case *Query:
		n.Statements = Rewrite(r, n.Statements).(Statements)

	case Statements:
		for i, s := range n {
			n[i] = Rewrite(r, s).(Statement)
		}

	case *SelectStatement:
		n.Fields = Rewrite(r, n.Fields).(Fields)
		n.Sources = Rewrite(r, n.Sources).(Sources)
		n.Condition = Rewrite(r, n.Condition).(Expr)

	case Fields:
		for i, f := range n {
			n[i] = Rewrite(r, f).(*Field)
		}

	case *Field:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case Dimensions:
		for i, d := range n {
			n[i] = Rewrite(r, d).(*Dimension)
		}

	case *Dimension:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case *BinaryExpr:
		n.LHS = Rewrite(r, n.LHS).(Expr)
		n.RHS = Rewrite(r, n.RHS).(Expr)

	case *ParenExpr:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	}

	return r.Rewrite(node)
}

// RewriteFunc rewrites a node hierarchy.
func RewriteFunc(node Node, fn func(Node) Node) Node {
	return Rewrite(rewriterFunc(fn), node)
}

type rewriterFunc func(Node) Node

func (fn rewriterFunc) Rewrite(n Node) Node { return fn(n) }

// Eval evaluates expr against a map.
func Eval(expr Expr, m map[string]interface{}) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *BinaryExpr:
		return evalBinaryExpr(expr, m)
	case *BooleanLiteral:
		return expr.Val
	case *NumberLiteral:
		return expr.Val
	case *ParenExpr:
		return Eval(expr.Expr, m)
	case *StringLiteral:
		return expr.Val
	case *VarRef:
		return m[expr.Val]
	default:
		return nil
	}
}

func evalBinaryExpr(expr *BinaryExpr, m map[string]interface{}) interface{} {
	lhs := Eval(expr.LHS, m)
	rhs := Eval(expr.RHS, m)

	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		rhs, _ := rhs.(bool)
		switch expr.Op {
		case AND:
			return lhs && rhs
		case OR:
			return lhs || rhs
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		}
	case float64:
		rhs, _ := rhs.(float64)
		switch expr.Op {
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		case LT:
			return lhs < rhs
		case LTE:
			return lhs <= rhs
		case GT:
			return lhs > rhs
		case GTE:
			return lhs >= rhs
		case ADD:
			return lhs + rhs
		case SUB:
			return lhs - rhs
		case MUL:
			return lhs * rhs
		case DIV:
			if rhs == 0 {
				return float64(0)
			}
			return lhs / rhs
		}
	case int64:
		// we parse all number literals as float 64, so we have to convert from
		// an interface to the float64, then cast to an int64 for comparison
		rhsf, _ := rhs.(float64)
		rhs := int64(rhsf)
		switch expr.Op {
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		case LT:
			return lhs < rhs
		case LTE:
			return lhs <= rhs
		case GT:
			return lhs > rhs
		case GTE:
			return lhs >= rhs
		case ADD:
			return lhs + rhs
		case SUB:
			return lhs - rhs
		case MUL:
			return lhs * rhs
		case DIV:
			if rhs == 0 {
				return int64(0)
			}
			return lhs / rhs
		}
	case string:
		rhs, _ := rhs.(string)
		switch expr.Op {
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		}
	}
	return nil
}
