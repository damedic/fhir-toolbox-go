package fhirpath

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/damedic/fhir-toolbox-go/fhirpath/internal/overflow"
	parser "github.com/damedic/fhir-toolbox-go/fhirpath/internal/parser"
)

type Element interface {
	// Children returns all child nodes with given names.
	//
	// If no name is passed, all children are returned.
	Children(name ...string) Collection
	ToBoolean(explicit bool) (v Boolean, ok bool, err error)
	ToString(explicit bool) (v String, ok bool, err error)
	ToInteger(explicit bool) (v Integer, ok bool, err error)
	ToLong(explicit bool) (v Long, ok bool, err error)
	ToDecimal(explicit bool) (v Decimal, ok bool, err error)
	ToDate(explicit bool) (v Date, ok bool, err error)
	ToTime(explicit bool) (v Time, ok bool, err error)
	ToDateTime(explicit bool) (v DateTime, ok bool, err error)
	ToQuantity(explicit bool) (v Quantity, ok bool, err error)
	Equal(other Element) (eq bool, ok bool)
	Equivalent(other Element) bool
	TypeInfo() TypeInfo
	json.Marshaler
	fmt.Stringer
}

// hasValuer is an interface for FHIR primitive elements that can report whether they have a value.
// FHIR primitives can have extensions without having a value (null in JSON).
type hasValuer interface {
	Element
	// HasValue returns true if the primitive element has a value (not just extensions).
	HasValue() bool
}

type cmpElement interface {
	Element
	// Cmp may return nil, because attempting to operate on quantities
	// with invalid units will result in empty ({ }).
	Cmp(other Element) (cmp int, ok bool, err error)
}

type multiplyElement interface {
	Element
	Multiply(ctx context.Context, other Element) (Element, error)
}

type divideElement interface {
	Element
	Divide(ctx context.Context, other Element) (Element, error)
}

type divElement interface {
	Element
	Div(ctx context.Context, other Element) (Element, error)
}

type modElement interface {
	Element
	Mod(ctx context.Context, other Element) (Element, error)
}

type addElement interface {
	Element
	Add(ctx context.Context, other Element) (Element, error)
}

type subtractElement interface {
	Element
	Subtract(ctx context.Context, other Element) (Element, error)
}

type apdContextKey struct{}

// WithAPDContext sets the apd.Context for Decimal operations in FHIRPath evaluations.
//
// The apd.Context controls the precision and rounding behavior of decimal operations.
// By default the evaluator uses defaultAPDContext, which keeps 34 significant decimal digits
// to exceed the minimum precision mandated by the FHIR spec for decimal values
// (FHIR R4, datatypes.html#decimal). Use WithAPDContext to override the precision when you
// need more headroom for intermediates or to experiment with tighter contexts.
//
// Example:
//
//	// Set precision to 10 digits
//	ctx := r4.Context()
//	ctx = fhirpath.WithAPDContext(ctx, apd.BaseContext.WithPrecision(10))
//
//	// Evaluate an expression with the specified precision
//	result, err := fhirpath.Evaluate(ctx, resource, expr)
func WithAPDContext(
	ctx context.Context,
	apdContext *apd.Context,
) context.Context {
	return context.WithValue(ctx, apdContextKey{}, apdContext)
}

// defaultAPDContext is the default precision context for decimal operations.
// Per FHIRPath spec, decimal values must support at least 18 decimal digits of precision.
// We configure apd to keep 34 digits (roughly Decimal128) so even values with large integer
// components retain at least 18 fractional digits during evaluation.
const defaultDecimalPrecision uint32 = 34

var defaultAPDContext = apd.BaseContext.WithPrecision(defaultDecimalPrecision)

func apdContext(ctx context.Context) *apd.Context {
	if ctx != nil {
		if apdContext, ok := ctx.Value(apdContextKey{}).(*apd.Context); ok && apdContext != nil {
			return apdContext
		}
	}
	return defaultAPDContext
}

type TypeInfo interface {
	Element
	QualifiedName() (TypeSpecifier, bool)
	BaseTypeName() (TypeSpecifier, bool)
}

type SimpleTypeInfo struct {
	defaultConversionError[SimpleTypeInfo]
	Namespace string        `json:"namespace"`
	Name      string        `json:"name"`
	BaseType  TypeSpecifier `json:"baseType"`
}

func (i SimpleTypeInfo) QualifiedName() (TypeSpecifier, bool) {
	return TypeSpecifier{Namespace: i.Namespace, Name: i.Name}, true
}
func (i SimpleTypeInfo) BaseTypeName() (TypeSpecifier, bool) {
	return i.BaseType, true
}
func (i SimpleTypeInfo) Children(name ...string) Collection {
	var children Collection
	if len(name) == 0 || slices.Contains(name, "namespace") {
		children = append(children, String(i.Namespace))
	}
	if len(name) == 0 || slices.Contains(name, "name") {
		children = append(children, String(i.Name))
	}
	if len(name) == 0 || slices.Contains(name, "baseType") {
		children = append(children, i.BaseType)
	}
	return children
}

func (i SimpleTypeInfo) Equal(other Element) (eq bool, ok bool) {
	return i == other, true
}
func (i SimpleTypeInfo) Equivalent(other Element) bool {
	eq, _ := i.Equal(other)
	return eq
}
func (i SimpleTypeInfo) TypeInfo() TypeInfo {
	return ClassInfo{
		Namespace: "System",
		Name:      "SimpleTypeInfo",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
		Element: []ClassInfoElement{
			{Name: "namespace", Type: TypeSpecifier{Namespace: "System", Name: "String"}},
			{Name: "name", Type: TypeSpecifier{Namespace: "System", Name: "String"}},
			{Name: "baseType", Type: TypeSpecifier{Namespace: "System", Name: "TypeSpecifier"}},
		},
	}
}
func (i SimpleTypeInfo) MarshalJSON() ([]byte, error) {
	type alias SimpleTypeInfo
	return json.Marshal(alias(i))
}
func (i SimpleTypeInfo) String() string {
	buf, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return "null"
	}
	return string(buf)
}

type ClassInfo struct {
	defaultConversionError[ClassInfo]
	Namespace string             `json:"namespace"`
	Name      string             `json:"name"`
	BaseType  TypeSpecifier      `json:"baseType"`
	Element   []ClassInfoElement `json:"element"`
}

func (i ClassInfo) QualifiedName() (TypeSpecifier, bool) {
	return TypeSpecifier{Namespace: i.Namespace, Name: i.Name}, true
}
func (i ClassInfo) BaseTypeName() (TypeSpecifier, bool) {
	return i.BaseType, true
}
func (i ClassInfo) Children(name ...string) Collection {
	var children Collection
	if len(name) == 0 || slices.Contains(name, "namespace") {
		children = append(children, String(i.Namespace))
	}
	if len(name) == 0 || slices.Contains(name, "name") {
		children = append(children, String(i.Name))
	}
	if len(name) == 0 || slices.Contains(name, "baseType") {
		children = append(children, i.BaseType)
	}
	if len(name) == 0 || slices.Contains(name, "element") {
		for _, e := range i.Element {
			children = append(children, e)
		}
	}
	return children
}
func (i ClassInfo) Equal(other Element) (eq bool, ok bool) {
	o, ok := other.(ClassInfo)
	if !ok {
		return false, true
	}
	if i.Namespace != o.Namespace {
		return false, true
	}
	if i.Name != o.Name {
		return false, true
	}
	if i.BaseType != o.BaseType {
		return false, true
	}
	if len(i.Element) != len(o.Element) {
		return false, true
	}
	for i, e := range i.Element {
		if e != o.Element[i] {
			return false, true
		}
	}
	return true, true
}
func (i ClassInfo) Equivalent(other Element) bool {
	eq, _ := i.Equal(other)
	return eq
}
func (i ClassInfo) TypeInfo() TypeInfo {
	return ClassInfo{
		Namespace: "System",
		Name:      "ClassInfo",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
		Element: []ClassInfoElement{
			{Name: "namespace", Type: TypeSpecifier{Namespace: "System", Name: "String"}},
			{Name: "name", Type: TypeSpecifier{Namespace: "System", Name: "String"}},
			{Name: "baseType", Type: TypeSpecifier{Namespace: "System", Name: "TypeSpecifier"}},
			{Name: "element", Type: TypeSpecifier{Namespace: "System", Name: "ClassInfoElement"}},
		},
	}
}
func (i ClassInfo) MarshalJSON() ([]byte, error) {
	type alias ClassInfo
	return json.Marshal(alias(i))
}
func (i ClassInfo) String() string {
	buf, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return "null"
	}
	return string(buf)
}

type ClassInfoElement struct {
	defaultConversionError[ClassInfoElement]
	Name       string        `json:"name"`
	Type       TypeSpecifier `json:"type"`
	IsOneBased bool          `json:"isOneBased"`
}

func (i ClassInfoElement) Children(name ...string) Collection {
	var children Collection
	if len(name) == 0 || slices.Contains(name, "name") {
		children = append(children, String(i.Name))
	}
	if len(name) == 0 || slices.Contains(name, "type") {
		children = append(children, i.Type)
	}
	if len(name) == 0 || slices.Contains(name, "isOneBased") {
		children = append(children, Boolean(i.IsOneBased))
	}
	return children
}
func (i ClassInfoElement) Equal(other Element) (eq bool, ok bool) {
	return i == other, true
}
func (i ClassInfoElement) Equivalent(other Element) bool {
	eq, _ := i.Equal(other)
	return eq
}
func (i ClassInfoElement) TypeInfo() TypeInfo {
	return ClassInfo{
		Namespace: "System",
		Name:      "ClassInfoElement",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
		Element: []ClassInfoElement{
			{Name: "name", Type: TypeSpecifier{Namespace: "System", Name: "String"}},
			{Name: "type", Type: TypeSpecifier{Namespace: "System", Name: "TypeSpecifier"}},
			{Name: "isOneBased", Type: TypeSpecifier{Namespace: "System", Name: "Boolean"}},
		},
	}
}
func (i ClassInfoElement) MarshalJSON() ([]byte, error) {
	type alias ClassInfoElement
	return json.Marshal(alias(i))
}
func (i ClassInfoElement) String() string {
	buf, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return "null"
	}
	return string(buf)
}

type ListTypeInfo struct {
	defaultConversionError[ListTypeInfo]
	ElementType TypeSpecifier `json:"elementType"`
}

func (i ListTypeInfo) QualifiedName() (TypeSpecifier, bool) {
	return TypeSpecifier{}, false
}
func (i ListTypeInfo) BaseTypeName() (TypeSpecifier, bool) {
	return TypeSpecifier{}, false
}
func (i ListTypeInfo) Children(name ...string) Collection {
	var children Collection
	if len(name) == 0 || slices.Contains(name, "elementType") {
		children = append(children, i.ElementType)
	}
	return children
}
func (i ListTypeInfo) Equal(other Element) (eq bool, ok bool) {
	return i == other, true
}
func (i ListTypeInfo) Equivalent(other Element) bool {
	eq, _ := i.Equal(other)
	return eq
}
func (i ListTypeInfo) TypeInfo() TypeInfo {
	return ClassInfo{
		Namespace: "System",
		Name:      "ListTypeInfo",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
		Element: []ClassInfoElement{
			{Name: "elementType", Type: TypeSpecifier{Namespace: "System", Name: "TypeSpecifier"}},
		},
	}
}
func (i ListTypeInfo) MarshalJSON() ([]byte, error) {
	type alias ListTypeInfo
	return json.Marshal(alias(i))
}
func (i ListTypeInfo) String() string {
	buf, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return "null"
	}
	return string(buf)
}

type TupleTypeInfo struct {
	defaultConversionError[TupleTypeInfo]
	Element []TupleTypeInfoElement `json:"element"`
}

func (i TupleTypeInfo) QualifiedName() (TypeSpecifier, bool) {
	return TypeSpecifier{}, false
}
func (i TupleTypeInfo) BaseTypeName() (TypeSpecifier, bool) {
	return TypeSpecifier{}, false
}
func (i TupleTypeInfo) Children(name ...string) Collection {
	var children Collection
	if len(name) == 0 || slices.Contains(name, "element") {
		for _, e := range i.Element {
			children = append(children, e)
		}
	}
	return children
}
func (i TupleTypeInfo) Equal(other Element) (eq bool, ok bool) {
	o, ok := other.(TupleTypeInfo)
	if !ok {
		return false, true
	}
	if len(i.Element) != len(o.Element) {
		return false, true
	}
	for i, e := range i.Element {
		if e != o.Element[i] {
			return false, true
		}
	}
	return true, true
}
func (i TupleTypeInfo) Equivalent(other Element) bool {
	eq, _ := i.Equal(other)
	return eq
}
func (i TupleTypeInfo) TypeInfo() TypeInfo {
	return ClassInfo{
		Namespace: "System",
		Name:      "TupleTypeInfo",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
		Element: []ClassInfoElement{
			{Name: "element", Type: TypeSpecifier{Namespace: "System", Name: "TupleTypeInfoElement"}},
		},
	}
}
func (i TupleTypeInfo) MarshalJSON() ([]byte, error) {
	type alias TupleTypeInfo
	return json.Marshal(alias(i))
}
func (i TupleTypeInfo) String() string {
	buf, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return "null"
	}
	return string(buf)
}

type TupleTypeInfoElement struct {
	defaultConversionError[TupleTypeInfoElement]
	Name       string        `json:"name"`
	Type       TypeSpecifier `json:"type"`
	IsOneBased bool          `json:"isOneBased"`
}

func (i TupleTypeInfoElement) Children(name ...string) Collection {
	var children Collection
	if len(name) == 0 || slices.Contains(name, "name") {
		children = append(children, String(i.Name))
	}
	if len(name) == 0 || slices.Contains(name, "type") {
		children = append(children, i.Type)
	}
	if len(name) == 0 || slices.Contains(name, "isOneBased") {
		children = append(children, Boolean(i.IsOneBased))
	}
	return children
}
func (i TupleTypeInfoElement) Equal(other Element) (eq bool, ok bool) {
	return i == other, true
}
func (i TupleTypeInfoElement) Equivalent(other Element) bool {
	eq, _ := i.Equal(other)
	return eq
}
func (i TupleTypeInfoElement) TypeInfo() TypeInfo {
	return ClassInfo{
		Namespace: "System",
		Name:      "TupleTypeInfoElement",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
		Element: []ClassInfoElement{
			{Name: "name", Type: TypeSpecifier{Namespace: "System", Name: "String"}},
			{Name: "type", Type: TypeSpecifier{Namespace: "System", Name: "TypeSpecifier"}},
			{Name: "isOneBased", Type: TypeSpecifier{Namespace: "System", Name: "Boolean"}},
		},
	}
}
func (i TupleTypeInfoElement) MarshalJSON() ([]byte, error) {
	type alias TupleTypeInfoElement
	return json.Marshal(alias(i))
}
func (i TupleTypeInfoElement) String() string {
	buf, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return "null"
	}
	return string(buf)
}

type TypeSpecifier struct {
	defaultConversionError[TypeSpecifier]
	Namespace string
	Name      string
	List      bool
}

func ParseTypeSpecifier(s string) TypeSpecifier {
	if strings.HasPrefix(s, "List<") {
		s = strings.TrimPrefix(s, "List<")
		s = strings.TrimSuffix(s, ">")
	}

	split := strings.SplitN(s, ".", 2)
	if len(split) == 1 {
		return TypeSpecifier{
			Name: strings.Trim(split[0], "`"),
		}
	} else {
		return TypeSpecifier{
			Namespace: strings.Trim(split[0], "`"),
			Name:      strings.Trim(split[1], "`"),
		}
	}
}

func (t TypeSpecifier) Children(name ...string) Collection {
	return nil
}
func (t TypeSpecifier) Equal(other Element) (eq bool, ok bool) {
	return t == other, true
}
func (t TypeSpecifier) Equivalent(other Element) bool {
	eq, _ := t.Equal(other)
	return eq
}
func (t TypeSpecifier) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "TypeSpecifier",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (t TypeSpecifier) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}
func (t TypeSpecifier) String() string {
	var s string
	if t.Namespace != "" {
		s = fmt.Sprintf("%s.%s", t.Namespace, t.Name)
	} else {
		s = t.Name
	}
	if t.List {
		return fmt.Sprintf("List<%s>", s)
	}
	return s
}

type namespaceKey struct{}

// WithNamespace installs the default namespace into the context.
func WithNamespace(
	ctx context.Context,
	namespace string,
) context.Context {
	return context.WithValue(ctx, namespaceKey{}, namespace)
}

func contextNamespace(ctx context.Context) string {
	ns, ok := ctx.Value(namespaceKey{}).(string)
	if !ok {
		return "System"
	}
	return ns
}

type knownTypesKey struct{}

// WithTypes installs the known types into the context.
func WithTypes(
	ctx context.Context,
	types []TypeInfo,
) context.Context {
	typeMap := knownTypes(ctx)
	for _, t := range types {
		qual, ok := t.QualifiedName()
		if !ok {
			continue
		}
		typeMap[qual] = t
	}
	return context.WithValue(ctx, knownTypesKey{}, typeMap)
}

func knownTypes(ctx context.Context) map[TypeSpecifier]TypeInfo {
	types, ok := ctx.Value(knownTypesKey{}).(map[TypeSpecifier]TypeInfo)
	if !ok {
		types = maps.Clone(systemTypesMap())
	}
	return types
}

var (
	systemTypes = []TypeInfo{
		Boolean(false).TypeInfo(),
		String("").TypeInfo(),
		Integer(0).TypeInfo(),
		Decimal{}.TypeInfo(),
		Date{}.TypeInfo(),
		Time{}.TypeInfo(),
		DateTime{}.TypeInfo(),
		Quantity{}.TypeInfo(),
	}
	systemTypesMap = sync.OnceValue(func() map[TypeSpecifier]TypeInfo {
		m := map[TypeSpecifier]TypeInfo{}
		for _, t := range systemTypes {
			q, ok := t.QualifiedName()
			if !ok {
				continue
			}
			m[q] = t
		}
		return m
	})
)

func resolveType(ctx context.Context, spec TypeSpecifier) (TypeInfo, bool) {
	if spec.Namespace == "" {
		// search context-specific namespace first
		info, ok := resolveType(ctx, TypeSpecifier{
			Namespace: contextNamespace(ctx),
			Name:      spec.Name,
		})
		if !ok {
			info, ok = resolveType(ctx, TypeSpecifier{
				Namespace: "System",
				Name:      spec.Name,
			})
		}
		return info, ok
	}

	typeMap := knownTypes(ctx)

	t, ok := typeMap[spec]
	return t, ok
}

func subTypeOf(ctx context.Context, target, isOf TypeInfo) bool {
	isOfQual, ok := isOf.QualifiedName()
	if !ok {
		// has no type
		return false
	}

	typQual, ok := target.QualifiedName()
	if ok && typQual == isOfQual {
		return true
	}
	baseQual, ok := target.BaseTypeName()
	if ok && baseQual == isOfQual {
		return true
	}

	baseType, ok := resolveType(ctx, baseQual)
	if !ok {
		return false
	}

	return subTypeOf(ctx, baseType, isOf)
}

func isType(ctx context.Context, target Element, isOf TypeSpecifier) (Element, error) {
	typ, ok := resolveType(ctx, isOf)
	if !ok {
		// Per FHIRPath spec, if the type cannot be resolved, is() returns false
		return Boolean(false), nil
	}

	// First check type hierarchy
	if subTypeOf(ctx, target.TypeInfo(), typ) {
		return Boolean(true), nil
	}

	targetQual, ok := target.TypeInfo().QualifiedName()
	if !ok {
		return Boolean(false), nil
	}

	// Check if this is a FHIR string-derived type checking against String
	if targetQual.Namespace == "FHIR" {
		isOfQual, ok := typ.QualifiedName()
		if ok && (isOfQual.Name == "String" || isOfQual.Name == "string") {
			// Only string-derived FHIR primitives should match System.String
			// Common FHIR string-derived types: code, uri, id, oid, uuid, url, canonical, etc.
			if _, ok, _ := target.ToString(false); ok {
				// Exclude non-string types that can convert to string
				switch targetQual.Name {
				case "boolean", "Boolean", "integer", "Integer",
					"decimal", "Decimal", "unsignedInt", "positiveInt":
					// These are numeric/boolean types, not string-derived
					return Boolean(false), nil
				default:
					// Assume it's a string-derived type
					return Boolean(true), nil
				}
			}
		}
	}

	return Boolean(false), nil
}

func asType(ctx context.Context, target Element, asOf TypeSpecifier) (Collection, error) {
	typ, ok := resolveType(ctx, asOf)
	if !ok {
		return nil, fmt.Errorf("can not resolve type `%s`", asOf)
	}
	if subTypeOf(ctx, target.TypeInfo(), typ) {
		return Collection{target}, nil
	} else {
		return nil, nil
	}
}

func elementTo[T Element](e Element, explicit bool) (v T, ok bool, err error) {
	switch any(v).(type) {
	case Boolean:
		v, ok, err := e.ToBoolean(explicit)
		return any(v).(T), ok, err
	case String:
		v, ok, err := e.ToString(explicit)
		return any(v).(T), ok, err
	case Integer:
		v, ok, err := e.ToInteger(explicit)
		return any(v).(T), ok, err
	case Decimal:
		v, ok, err := e.ToDecimal(explicit)
		return any(v).(T), ok, err
	case Date:
		v, ok, err := e.ToDate(explicit)
		return any(v).(T), ok, err
	case Time:
		v, ok, err := e.ToTime(explicit)
		return any(v).(T), ok, err
	case DateTime:
		v, ok, err := e.ToDateTime(explicit)
		return any(v).(T), ok, err
	case Quantity:
		v, ok, err := e.ToQuantity(explicit)
		return any(v).(T), ok, err
	default:
		return v, false, fmt.Errorf("can not convert to type %T", v)
	}
}

func toPrimitive(e Element) (Element, bool) {
	if p, ok, err := e.ToBoolean(false); err == nil && ok {
		return p, true
	}
	if p, ok, err := e.ToString(false); err == nil && ok {
		return p, true
	}
	if p, ok, err := e.ToInteger(false); err == nil && ok {
		return p, true
	}
	switch v := e.(type) {
	case Long:
		return v, true
	case *Long:
		if v != nil {
			return *v, true
		}
	}
	if p, ok, err := e.ToDecimal(false); err == nil && ok {
		return p, true
	}
	if p, ok, err := e.ToDateTime(false); err == nil && ok {
		return p, true
	}
	if p, ok, err := e.ToDate(false); err == nil && ok {
		return p, true
	}
	if p, ok, err := e.ToTime(false); err == nil && ok {
		return p, true
	}
	if p, ok, err := e.ToQuantity(false); err == nil && ok {
		return p, true
	}
	return nil, false
}

type Collection []Element

func (c Collection) Equal(other Collection) (eq bool, ok bool) {
	if len(c) == 0 || len(other) == 0 {
		return false, false
	}
	if len(c) != len(other) {
		return false, true
	}
	for i, e := range c {
		eq, ok := e.Equal(other[i])
		if !ok || !eq {
			return false, ok
		}
	}
	return true, true
}

func (c Collection) Equivalent(other Collection) bool {
	if len(c) == 0 && len(other) == 0 {
		return true
	}
	if len(c) != len(other) {
		return false
	}

outer:
	for _, e := range c {
		for _, o := range other {
			if e.Equivalent(o) {
				continue outer
			}
		}
		return false
	}
	return true
}

func (c Collection) Cmp(other Collection) (cmp int, ok bool, err error) {
	if len(c) == 0 || len(other) == 0 {
		return 0, false, nil
	}
	if len(c) != 1 || len(other) != 1 {
		return 0, false, fmt.Errorf("can not compare collections with len != 1: %v and %v", c, other)
	}

	left, ok := c[0].(cmpElement)
	if !ok {
		primitive, _ := toPrimitive(c[0])
		left, ok = primitive.(cmpElement)
	}
	if !ok {
		return 0, false, errors.New("only strings, integers, decimals, quantities, dates, datetimes and times can be compared")
	}
	right := other[0]

	return left.Cmp(right)
}
func (c Collection) Union(other Collection) Collection {
	// If the input collection is empty, return the other collection
	if len(c) == 0 {
		return slices.Clone(other)
	}

	// If the other collection is empty, return the input collection
	if len(other) == 0 {
		return slices.Clone(c)
	}

	var union Collection

	// add elements from the first collection
outer1:
	for _, e := range c {
		for _, u := range union {
			eq, ok := e.Equal(u)
			if ok && eq {
				continue outer1
			}
		}
		union = append(union, e)
	}

	// add elements from the second collection
outer2:
	for _, e := range other {
		for _, u := range union {
			eq, ok := e.Equal(u)
			if ok && eq {
				continue outer2
			}
		}
		union = append(union, e)
	}

	return union
}

func (c Collection) Combine(other Collection) Collection {
	// If the input collection is empty, return the other collection
	if len(c) == 0 {
		return slices.Clone(other)
	}

	// If the other collection is empty, return the input collection
	if len(other) == 0 {
		return slices.Clone(c)
	}

	// Combine the two collections without eliminating duplicates
	combined := slices.Clone(c)
	combined = append(combined, other...)

	return combined
}
func (c Collection) Contains(element Element) bool {
	for _, e := range c {
		eq, ok := e.Equal(element)
		if ok && eq {
			return true
		}
	}
	return false
}

func (c Collection) Multiply(ctx context.Context, other Collection) (Collection, error) {
	if len(c) == 0 || len(other) == 0 {
		return nil, nil
	}
	if len(c) != 1 {
		return nil, fmt.Errorf("left value for multiplication has len != 1: %v", c)
	}
	if len(other) != 1 {
		return nil, fmt.Errorf("right value for multiplication has len != 1: %v", other)
	}

	left, ok := c[0].(multiplyElement)
	if !ok {
		primitive, _ := toPrimitive(c[0])
		left, ok = primitive.(multiplyElement)
	}
	if !ok {
		return nil, errors.New("can only multiply Integer, Long, Decimal or Quantity")
	}
	right := other[0]

	res, err := left.Multiply(ctx, right)
	if err != nil {
		return nil, err
	}
	return Collection{res}, nil
}

func (c Collection) Divide(ctx context.Context, other Collection) (Collection, error) {
	if len(c) == 0 || len(other) == 0 {
		return nil, nil
	}
	if len(c) != 1 {
		return nil, fmt.Errorf("left value for division has len != 1: %v", c)
	}
	if len(other) != 1 {
		return nil, fmt.Errorf("right value for division has len != 1: %v", other)
	}

	left, ok := c[0].(divideElement)
	if !ok {
		primitive, _ := toPrimitive(c[0])
		left, ok = primitive.(divideElement)
	}
	if !ok {
		return nil, errors.New("can only divide Integer, Long, Decimal or Quantity")
	}
	right := other[0]

	res, err := left.Divide(ctx, right)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return Collection{res}, nil
}

func (c Collection) Div(ctx context.Context, other Collection) (Collection, error) {
	if len(c) == 0 || len(other) == 0 {
		return nil, nil
	}
	if len(c) != 1 {
		return nil, fmt.Errorf("left value for div has len != 1: %v", c)
	}
	if len(other) != 1 {
		return nil, fmt.Errorf("right value for div has len != 1: %v", other)
	}

	left, ok := c[0].(divElement)
	if !ok {
		primitive, _ := toPrimitive(c[0])
		left, ok = primitive.(divElement)
	}
	if !ok {
		return nil, errors.New("can only div Integer, Long, Decimal")
	}
	right := other[0]

	res, err := left.Div(ctx, right)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return Collection{res}, nil
}

func (c Collection) Mod(ctx context.Context, other Collection) (Collection, error) {
	if len(c) == 0 || len(other) == 0 {
		return nil, nil
	}
	if len(c) != 1 {
		return nil, fmt.Errorf("left value for div has len != 1: %v", c)
	}
	if len(other) != 1 {
		return nil, fmt.Errorf("right value for div has len != 1: %v", other)
	}

	left, ok := c[0].(modElement)
	if !ok {
		primitive, _ := toPrimitive(c[0])
		left, ok = primitive.(modElement)
	}
	if !ok {
		return nil, errors.New("can only div Integer, Long, Decimal")
	}
	right := other[0]

	res, err := left.Mod(ctx, right)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return Collection{res}, nil
}

func (c Collection) Add(ctx context.Context, other Collection) (Collection, error) {
	if len(c) == 0 || len(other) == 0 {
		return nil, nil
	}
	if len(c) != 1 {
		return nil, fmt.Errorf("left value for addition has len != 1: %v", c)
	}
	if len(other) != 1 {
		return nil, fmt.Errorf("right value for addition has len != 1: %v", other)
	}

	left, ok := c[0].(addElement)
	if !ok {
		primitive, _ := toPrimitive(c[0])
		left, ok = primitive.(addElement)
	}
	if !ok {
		return nil, errors.New("can only div Integer, Long, Decimal, Quantity and String")
	}
	right := other[0]

	res, err := left.Add(ctx, right)
	if err != nil {
		return nil, err
	}
	return Collection{res}, nil
}

func (c Collection) Subtract(ctx context.Context, other Collection) (Collection, error) {
	if len(c) == 0 || len(other) == 0 {
		return nil, nil
	}
	if len(c) != 1 {
		return nil, fmt.Errorf("left value for subtract has len != 1: %v", c)
	}
	if len(other) != 1 {
		return nil, fmt.Errorf("right value for subtract has len != 1: %v", other)
	}

	left, ok := c[0].(subtractElement)
	if !ok {
		primitive, _ := toPrimitive(c[0])
		left, ok = primitive.(subtractElement)
	}
	if !ok {
		return nil, errors.New("can only div Integer, Long, Decimal, Quantity")
	}
	right := other[0]

	res, err := left.Subtract(ctx, right)
	if err != nil {
		return nil, err
	}
	return Collection{res}, nil
}

func (c Collection) Concat(ctx context.Context, other Collection) (Collection, error) {
	if len(c) > 1 {
		return nil, fmt.Errorf("left value for concat has len > 1: %v", c)
	}
	if len(other) > 1 {
		return nil, fmt.Errorf("right value for concat has len > 1: %v", other)
	}
	if len(c) == 0 && len(other) == 0 {
		return Collection{String("")}, nil
	}

	var left, right String
	if len(c) == 1 {
		// Use elementTo for implicit conversion (e.g., FHIR primitives to String)
		s, ok, err := elementTo[String](c[0], false)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("can only concat String, got left %T: %v", c[0], c[0])
		}
		left = s
	}
	if len(other) == 1 {
		// Use elementTo for implicit conversion (e.g., FHIR primitives to String)
		s, ok, err := elementTo[String](other[0], false)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("can only concat String, got right %T: %v", other[0], other[0])
		}
		right = s
	}
	return Collection{left + right}, nil
}

func (c Collection) String() string {
	if len(c) == 0 {
		return "{ }"
	}

	var b strings.Builder
	b.WriteString("{ ")

	for _, e := range c[:len(c)-1] {
		// strings.Builder Write implementation does not return error
		_, _ = fmt.Fprint(&b, e, ", ")
	}
	_, _ = fmt.Fprint(&b, c[len(c)-1])

	b.WriteString(" }")
	return b.String()
}

type Boolean bool

func (b Boolean) Children(name ...string) Collection {
	return nil
}

func (b Boolean) ToBoolean(explicit bool) (v Boolean, ok bool, err error) {
	return b, true, nil
}
func (b Boolean) ToString(explicit bool) (v String, ok bool, err error) {
	if explicit {
		return String(b.String()), true, nil
	}
	return "", false, implicitConversionError[Boolean, String](b)
}
func (b Boolean) ToInteger(explicit bool) (v Integer, ok bool, err error) {
	if explicit {
		if b {
			return 1, true, nil
		} else {
			return 0, true, nil
		}
	}
	return 0, false, implicitConversionError[Boolean, Integer](b)
}
func (b Boolean) ToLong(explicit bool) (v Long, ok bool, err error) {
	if explicit {
		if b {
			return 1, true, nil
		}
		return 0, true, nil
	}
	return 0, false, implicitConversionError[Boolean, Long](b)
}
func (b Boolean) ToDecimal(explicit bool) (v Decimal, ok bool, err error) {
	if explicit {
		if b {
			return Decimal{Value: apd.New(10, -1)}, true, nil
		} else {
			return Decimal{Value: apd.New(00, -1)}, true, nil
		}
	}
	return Decimal{}, false, implicitConversionError[Boolean, Decimal](b)
}

func (b Boolean) ToDate(explicit bool) (v Date, ok bool, err error) {
	return Date{}, false, conversionError[Boolean, Date]()
}
func (b Boolean) ToTime(explicit bool) (v Time, ok bool, err error) {
	return Time{}, false, conversionError[Boolean, Time]()
}
func (b Boolean) ToDateTime(explicit bool) (v DateTime, ok bool, err error) {
	return DateTime{}, false, conversionError[Boolean, DateTime]()
}
func (b Boolean) ToQuantity(explicit bool) (v Quantity, ok bool, err error) {
	if explicit {
		if b {
			return Quantity{Value: Decimal{Value: apd.New(10, -1)}, Unit: "1"}, true, nil
		} else {
			return Quantity{Value: Decimal{Value: apd.New(00, -1)}, Unit: "1"}, true, nil
		}
	}
	return Quantity{}, false, conversionError[Boolean, Quantity]()
}
func (b Boolean) Equal(other Element) (eq bool, ok bool) {
	o, ok, err := other.ToBoolean(false)
	if err == nil && ok {
		return b == o, true
	}
	if _, isString := other.(String); isString {
		return other.Equal(b)
	}
	if _, isStringPtr := other.(*String); isStringPtr {
		return other.Equal(b)
	}
	return false, true
}
func (b Boolean) Equivalent(other Element) bool {
	eq, ok := b.Equal(other)
	return ok && eq
}
func (b Boolean) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "Boolean",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (b Boolean) MarshalJSON() ([]byte, error) {
	return json.Marshal(bool(b))
}
func (b Boolean) String() string {
	return strconv.FormatBool(bool(b))
}

type String string

func (s String) Children(name ...string) Collection {
	return nil
}

func (s String) ToBoolean(explicit bool) (v Boolean, ok bool, err error) {
	if explicit {
		if slices.Contains([]string{"true", "t", "yes", "y", "1", "1.0"}, strings.ToLower(string(s))) {
			return true, true, nil
		} else if slices.Contains([]string{"false", "f", "no", "n", "0", "0.0"}, strings.ToLower(string(s))) {
			return false, true, nil
		} else {
			return false, false, nil
		}
	}
	return false, false, implicitConversionError[String, Boolean](s)
}
func (s String) ToString(explicit bool) (v String, ok bool, err error) {
	return s, true, nil
}
func (s String) ToInteger(explicit bool) (v Integer, ok bool, err error) {
	if explicit {
		val, err := strconv.ParseInt(string(s), 10, 32)
		if err != nil {
			return 0, false, nil
		}
		return Integer(val), true, nil
	}
	return 0, false, implicitConversionError[String, Integer](s)
}
func (s String) ToLong(explicit bool) (v Long, ok bool, err error) {
	if explicit {
		val, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			return 0, false, nil
		}
		return Long(val), true, nil
	}
	return 0, false, implicitConversionError[String, Long](s)
}
func (s String) ToDecimal(explicit bool) (v Decimal, ok bool, err error) {
	if explicit {
		d, _, err := apd.NewFromString(string(s))
		if err != nil {
			return Decimal{}, false, nil
		}
		return Decimal{Value: d}, true, nil
	}
	return Decimal{}, false, implicitConversionError[String, Decimal](s)
}
func (s String) ToDate(explicit bool) (v Date, ok bool, err error) {
	if explicit {
		d, err := ParseDate(string(s))
		if err != nil {
			return Date{}, false, nil
		}
		return d, true, nil
	}
	return Date{}, false, implicitConversionError[String, Date](s)
}
func (s String) ToTime(explicit bool) (v Time, ok bool, err error) {
	if explicit {
		d, err := parseTime(string(s), false)
		if err != nil {
			return Time{}, false, nil
		}
		return d, true, nil
	}
	return Time{}, false, implicitConversionError[String, Time](s)
}
func (s String) ToDateTime(explicit bool) (v DateTime, ok bool, err error) {
	if explicit {
		d, err := ParseDateTime(string(s))
		if err != nil {
			return DateTime{}, false, nil
		}
		return d, true, nil
	}
	return DateTime{}, false, implicitConversionError[String, DateTime](s)
}
func (s String) ToQuantity(explicit bool) (v Quantity, ok bool, err error) {
	q, err := ParseQuantity(string(s))
	if err != nil {
		return Quantity{}, false, nil
	}
	return q, true, nil
}
func (s String) Equal(other Element) (eq bool, ok bool) {
	o, ok, err := other.ToString(false)
	if err == nil && ok {
		return s == o, true
	}
	return false, ok && err == nil
}

var whitespaceReplaceRegex = regexp.MustCompile("[\t\r\n]")

func (s String) Equivalent(other Element) bool {
	o, ok, err := other.ToString(false)
	if err == nil && ok {
		return whitespaceReplaceRegex.ReplaceAllString(strings.ToLower(string(s)), " ") ==
			whitespaceReplaceRegex.ReplaceAllString(strings.ToLower(string(o)), " ")
	}
	return false
}
func (s String) Cmp(other Element) (cmp int, ok bool, err error) {
	o, ok, err := other.ToString(false)
	if err != nil || !ok {
		return 0, false, fmt.Errorf("can not compare String to %T, left: %v right: %v", other, s, other)
	}
	return strings.Compare(string(s), string(o)), true, nil
}
func (s String) Add(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToString(false)
	if err != nil {
		return nil, fmt.Errorf("can not add %T to String, %v + %v", other, s, other)
	}
	if !ok {
		return nil, nil
	}
	return s + o, nil
}
func (s String) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "String",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (s String) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(s))
}
func (s String) String() string {
	return fmt.Sprintf("'%s'", string(s))
}

func isStringish(e Element) bool {
	switch e.(type) {
	case String, *String:
		return true
	default:
		return false
	}
}

func canDelegateNumeric(e Element) bool {
	switch e.(type) {
	case Decimal, *Decimal, Quantity, *Quantity, String, *String, Long, *Long:
		return true
	default:
		return false
	}
}

func canDelegateDecimal(e Element) bool {
	switch e.(type) {
	case Quantity, *Quantity, String, *String, Long, *Long:
		return true
	default:
		return false
	}
}

func delegatesToDateTime(e Element) bool {
	switch e.(type) {
	case DateTime, *DateTime:
		return true
	default:
		return false
	}
}

var (
	// escapes not handled by strconv.Unquote
	unescapeReplacer = strings.NewReplacer(
		`\'`, `'`,
		"\\`", "`",
		`\/`, `/`,
	)
)

func unescape(s string) (string, error) {
	// First, handle FHIRPath-specific escapes
	unescaped := unescapeReplacer.Replace(s)

	// strconv.Unquote expects a Go string literal with double quotes
	// We need to escape any unescaped double quotes and control characters
	// In FHIRPath, strings use single quotes, so double quotes inside don't need escaping
	// But for strconv.Unquote (which expects double-quoted strings), we need to escape them
	var builder strings.Builder
	builder.WriteByte('"')
	for i := 0; i < len(unescaped); i++ {
		c := unescaped[i]
		// Check if already escaped
		alreadyEscaped := i > 0 && unescaped[i-1] == '\\'

		switch c {
		case '"':
			if alreadyEscaped {
				// Already escaped, keep as is
				builder.WriteByte(c)
			} else {
				// Not escaped, escape it for strconv.Unquote
				builder.WriteString(`\"`)
			}
		case '\n':
			if !alreadyEscaped {
				// Escape newline
				builder.WriteString(`\n`)
			} else {
				builder.WriteByte(c)
			}
		case '\r':
			if !alreadyEscaped {
				// Escape carriage return
				builder.WriteString(`\r`)
			} else {
				builder.WriteByte(c)
			}
		case '\t':
			if !alreadyEscaped {
				// Escape tab
				builder.WriteString(`\t`)
			} else {
				builder.WriteByte(c)
			}
		default:
			builder.WriteByte(c)
		}
	}
	builder.WriteByte('"')

	// handles \", \r, \n, \t, \f, \\, \uXXXX
	return strconv.Unquote(builder.String())
}

type Integer int32

func (i Integer) Children(name ...string) Collection {
	return nil
}

func (i Integer) ToBoolean(explicit bool) (v Boolean, ok bool, err error) {
	if explicit {
		switch i {
		case 0:
			return false, true, nil
		case 1:
			return true, true, nil
		default:
			return false, false, nil
		}
	}
	return false, false, implicitConversionError[Integer, Boolean](i)
}
func (i Integer) ToString(explicit bool) (v String, ok bool, err error) {
	return String(i.String()), true, nil
}
func (i Integer) ToInteger(explicit bool) (v Integer, ok bool, err error) {
	return i, true, nil
}
func (i Integer) ToLong(explicit bool) (v Long, ok bool, err error) {
	return Long(i), true, nil
}
func (i Integer) ToDecimal(explicit bool) (v Decimal, ok bool, err error) {
	return Decimal{Value: apd.New(int64(i), 0)}, true, nil
}
func (i Integer) ToDate(explicit bool) (v Date, ok bool, err error) {
	return Date{}, false, conversionError[Integer, Date]()
}
func (i Integer) ToTime(explicit bool) (v Time, ok bool, err error) {
	return Time{}, false, conversionError[Integer, Time]()
}
func (i Integer) ToDateTime(explicit bool) (v DateTime, ok bool, err error) {
	return DateTime{}, false, conversionError[Integer, DateTime]()
}
func (i Integer) ToQuantity(explicit bool) (v Quantity, ok bool, err error) {
	return Quantity{
		Value: Decimal{
			Value: apd.New(int64(i), 0),
		},
		Unit: "1",
	}, true, nil
}
func (i Integer) Equal(other Element) (eq bool, ok bool) {
	o, ok, err := other.ToInteger(false)
	if err == nil && ok {
		return i == o, true
	}
	if canDelegateNumeric(other) {
		return other.Equal(i)
	}
	return false, true
}
func (i Integer) Equivalent(other Element) bool {
	eq, ok := i.Equal(other)
	return ok && eq
}
func (i Integer) Cmp(other Element) (cmp int, ok bool, err error) {
	d, _, _ := i.ToDecimal(false)
	if _, isLong := other.(Long); isLong {
		return Long(i).Cmp(other)
	}
	cmp, ok, err = d.Cmp(other)
	if err != nil || !ok {
		return 0, false, fmt.Errorf("can not compare Integer to %T, left: %v right: %v", other, i, other)
	}
	return cmp, true, nil
}
func (i Integer) Multiply(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Integer:
		result, ok := overflow.Mul[int32](int32(i), int32(o))
		if !ok {
			return nil, nil
		}
		return Integer(result), nil
	case Long:
		return Long(i).Multiply(ctx, o)
	case Decimal:
		d, _, _ := i.ToDecimal(false)
		return d.Multiply(ctx, o)
	}
	return nil, fmt.Errorf("can not multiply Integer with %T: %v * %v", other, i, other)
}
func (i Integer) Divide(ctx context.Context, other Element) (Element, error) {
	d, _, _ := i.ToDecimal(false)
	return d.Divide(ctx, other)
}
func (i Integer) Div(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Integer:
		result, ok := overflow.Div[int32](int32(i), int32(o))
		if !ok {
			return nil, nil
		}
		return Integer(result), nil
	case Long:
		return Long(i).Div(ctx, o)
	case Decimal:
		d, _, _ := i.ToDecimal(false)
		return d.Div(ctx, o)
	}
	return nil, fmt.Errorf("can not div Integer with %T: %v div %v", other, i, other)
}
func (i Integer) Mod(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Integer:
		result, ok := overflow.Mod[int32](int32(i), int32(o))
		if !ok {
			return nil, nil
		}
		return Integer(result), nil
	case Long:
		return Long(i).Mod(ctx, o)
	case Decimal:
		d, _, _ := i.ToDecimal(false)
		return d.Mod(ctx, o)
	}
	return nil, fmt.Errorf("can not mod Integer with %T: %v mod %v", other, i, other)
}
func (i Integer) Add(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Integer:
		result, ok := overflow.Add[int32](int32(i), int32(o))
		if !ok {
			return nil, nil
		}
		return Integer(result), nil
	case Long:
		return Long(i).Add(ctx, o)
	case Decimal:
		d, _, _ := i.ToDecimal(false)
		return d.Add(ctx, o)
	}
	return nil, fmt.Errorf("can not add Integer and %T: %v + %v", other, i, other)
}
func (i Integer) Subtract(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Integer:
		result, ok := overflow.Sub[int32](int32(i), int32(o))
		if !ok {
			return nil, nil
		}
		return Integer(result), nil
	case Long:
		return Long(i).Subtract(ctx, o)
	case Decimal:
		d, _, _ := i.ToDecimal(false)
		return d.Subtract(ctx, o)
	}
	return nil, fmt.Errorf("can not subtract %T from Integer: %v - %v", other, i, other)
}
func (i Integer) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "Integer",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (i Integer) MarshalJSON() ([]byte, error) {
	return json.Marshal(int32(i))
}
func (i Integer) String() string {
	return strconv.Itoa(int(i))
}

type Long int64

func (l Long) Children(name ...string) Collection {
	return nil
}
func (l Long) ToBoolean(explicit bool) (v Boolean, ok bool, err error) {
	if explicit {
		switch l {
		case 0:
			return false, true, nil
		case 1:
			return true, true, nil
		default:
			return false, false, nil
		}
	}
	return false, false, implicitConversionError[Long, Boolean](l)
}
func (l Long) ToString(explicit bool) (v String, ok bool, err error) {
	return String(strconv.FormatInt(int64(l), 10)), true, nil
}
func (l Long) ToInteger(explicit bool) (v Integer, ok bool, err error) {
	if !explicit {
		return 0, false, implicitConversionError[Long, Integer](l)
	}
	if l < math.MinInt32 || l > math.MaxInt32 {
		return 0, false, fmt.Errorf("long %d cannot be represented as Integer", l)
	}
	return Integer(l), true, nil
}
func (l Long) ToLong(explicit bool) (v Long, ok bool, err error) {
	return l, true, nil
}
func (l Long) ToDecimal(explicit bool) (v Decimal, ok bool, err error) {
	return Decimal{Value: apd.New(int64(l), 0)}, true, nil
}
func (l Long) ToDate(explicit bool) (v Date, ok bool, err error) {
	return Date{}, false, conversionError[Long, Date]()
}
func (l Long) ToTime(explicit bool) (v Time, ok bool, err error) {
	return Time{}, false, conversionError[Long, Time]()
}
func (l Long) ToDateTime(explicit bool) (v DateTime, ok bool, err error) {
	return DateTime{}, false, conversionError[Long, DateTime]()
}
func (l Long) ToQuantity(explicit bool) (v Quantity, ok bool, err error) {
	return Quantity{
		Value: Decimal{Value: apd.New(int64(l), 0)},
		Unit:  "1",
	}, true, nil
}
func (l Long) Equal(other Element) (eq bool, ok bool) {
	switch o := other.(type) {
	case Long:
		return l == o, true
	case *Long:
		if o == nil {
			return false, true
		}
		return l == *o, true
	case Integer:
		return l == Long(o), true
	case *Integer:
		if o == nil {
			return false, true
		}
		return l == Long(*o), true
	}
	if canDelegateNumeric(other) {
		return other.Equal(l)
	}
	return false, true
}
func (l Long) Equivalent(other Element) bool {
	eq, ok := l.Equal(other)
	return ok && eq
}
func (l Long) Cmp(other Element) (cmp int, ok bool, err error) {
	switch o := other.(type) {
	case Long:
		switch {
		case l < o:
			return -1, true, nil
		case l > o:
			return 1, true, nil
		default:
			return 0, true, nil
		}
	case Integer:
		switch {
		case l < Long(o):
			return -1, true, nil
		case l > Long(o):
			return 1, true, nil
		default:
			return 0, true, nil
		}
	}
	d, _, _ := l.ToDecimal(false)
	return d.Cmp(other)
}
func (l Long) Multiply(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Long:
		result, ok := overflow.Mul[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Integer:
		result, ok := overflow.Mul[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Decimal:
		d, _, _ := l.ToDecimal(false)
		return d.Multiply(ctx, o)
	}
	return nil, fmt.Errorf("can not multiply Long with %T: %v * %v", other, l, other)
}
func (l Long) Divide(ctx context.Context, other Element) (Element, error) {
	d, _, _ := l.ToDecimal(false)
	return d.Divide(ctx, other)
}
func (l Long) Div(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Long:
		result, ok := overflow.Div[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Integer:
		result, ok := overflow.Div[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Decimal:
		d, _, _ := l.ToDecimal(false)
		return d.Div(ctx, o)
	}
	return nil, fmt.Errorf("can not div Long with %T: %v div %v", other, l, other)
}
func (l Long) Mod(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Long:
		result, ok := overflow.Mod[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Integer:
		result, ok := overflow.Mod[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Decimal:
		d, _, _ := l.ToDecimal(false)
		return d.Mod(ctx, o)
	}
	return nil, fmt.Errorf("can not mod Long with %T: %v mod %v", other, l, other)
}
func (l Long) Add(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Long:
		result, ok := overflow.Add[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Integer:
		result, ok := overflow.Add[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Decimal:
		d, _, _ := l.ToDecimal(false)
		return d.Add(ctx, o)
	}
	return nil, fmt.Errorf("can not add Long and %T: %v + %v", other, l, other)
}
func (l Long) Subtract(ctx context.Context, other Element) (Element, error) {
	switch o := other.(type) {
	case Long:
		result, ok := overflow.Sub[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Integer:
		result, ok := overflow.Sub[int64](int64(l), int64(o))
		if !ok {
			return nil, nil
		}
		return Long(result), nil
	case Decimal:
		d, _, _ := l.ToDecimal(false)
		return d.Subtract(ctx, o)
	}
	return nil, fmt.Errorf("can not subtract %T from Long: %v - %v", other, l, other)
}
func (l Long) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "Long",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (l Long) MarshalJSON() ([]byte, error) {
	return json.Marshal(int64(l))
}
func (l Long) String() string {
	return fmt.Sprintf("%dL", l)
}

type Decimal struct {
	defaultConversionError[Decimal]
	Value *apd.Decimal
}

func (d Decimal) Children(name ...string) Collection {
	return nil
}

func (d Decimal) ToBoolean(explicit bool) (v Boolean, ok bool, err error) {
	if explicit {
		if d.Value.Cmp(apd.New(1, 0)) == 0 {
			return true, true, nil
		} else if d.Value.Cmp(apd.New(0, 0)) == 0 {
			return false, true, nil
		} else {
			return false, false, nil
		}
	}
	return false, false, implicitConversionError[Decimal, Boolean](d)
}
func (d Decimal) ToString(explicit bool) (v String, ok bool, err error) {
	return String(d.String()), true, nil
}
func (d Decimal) ToDecimal(explicit bool) (v Decimal, ok bool, err error) {
	return d, true, nil
}
func (d Decimal) ToQuantity(explicit bool) (v Quantity, ok bool, err error) {
	return Quantity{
		Value: d,
		Unit:  "1",
	}, true, nil
}
func (d Decimal) Equal(other Element) (eq bool, ok bool) {
	o, ok, err := other.ToDecimal(false)
	if err == nil && ok {
		return d.Value.Cmp(o.Value) == 0, true
	}
	if canDelegateDecimal(other) {
		return other.Equal(d)
	}
	return false, true
}
func (d Decimal) Equivalent(other Element) bool {
	o, ok, err := other.ToDecimal(false)
	if err == nil && ok {
		prec := uint32(min(d.Value.NumDigits(), o.Value.NumDigits()))
		ctx := apd.BaseContext.WithPrecision(prec)
		var a, b apd.Decimal
		_, err = ctx.Round(&a, d.Value)
		if err != nil {
			return false
		}
		_, err = ctx.Round(&b, o.Value)
		if err != nil {
			return false
		}
		return a.Cmp(&b) == 0
	}
	if canDelegateDecimal(other) {
		return other.Equivalent(d)
	}
	return false
}
func (d Decimal) Cmp(other Element) (cmp int, ok bool, err error) {
	o, ok, err := other.ToDecimal(false)
	if err != nil || !ok {
		return 0, false, fmt.Errorf("can not compare Decimal to %T, left: %v right: %v", other, d, other)
	}
	return d.Value.Cmp(o.Value), true, nil
}
func (d Decimal) Multiply(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToDecimal(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not multiply Decimal with %T: %v * %v", other, d, other)
	}
	var res apd.Decimal
	_, err = apdContext(ctx).Mul(&res, d.Value, o.Value)
	if err != nil {
		return nil, err
	}
	return Decimal{Value: &res}, nil
}
func (d Decimal) Divide(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToDecimal(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not divide Decimal with %T: %v / %v", other, d, other)
	}
	if o.Value.IsZero() {
		return nil, nil
	}
	var res apd.Decimal
	_, err = apdContext(ctx).Quo(&res, d.Value, o.Value)
	if err != nil {
		return nil, err
	}
	return Decimal{Value: &res}, nil
}
func (d Decimal) Div(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToDecimal(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not div Decimal with %T: %v div %v", other, d, other)
	}
	if o.Value.IsZero() {
		return nil, nil
	}
	var res apd.Decimal
	_, err = apdContext(ctx).QuoInteger(&res, d.Value, o.Value)
	if err != nil {
		return nil, err
	}
	return Decimal{Value: &res}, nil
}
func (d Decimal) Mod(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToDecimal(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not mod Decimal with %T: %v mod %v", other, d, other)
	}
	if o.Value.IsZero() {
		return nil, nil
	}
	var res apd.Decimal
	_, err = apdContext(ctx).Rem(&res, d.Value, o.Value)
	if err != nil {
		return nil, err
	}
	return Decimal{Value: &res}, nil
}
func (d Decimal) Add(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToDecimal(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not add Decimal and %T: %v + %v", other, d, other)
	}
	if o.Value.IsZero() {
		return nil, nil
	}
	var res apd.Decimal
	_, err = apdContext(ctx).Add(&res, d.Value, o.Value)
	if err != nil {
		return nil, err
	}
	return Decimal{Value: &res}, nil
}
func (d Decimal) Subtract(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToDecimal(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not subtract %T from Decimal: %v - %v", other, d, other)
	}
	if o.Value.IsZero() {
		return nil, nil
	}
	var res apd.Decimal
	_, err = apdContext(ctx).Sub(&res, d.Value, o.Value)
	if err != nil {
		return nil, err
	}
	return Decimal{Value: &res}, nil
}

// Precision returns the number of decimal places in the decimal value
func (d Decimal) Precision() int {
	if d.Value.Exponent < 0 {
		return int(-d.Value.Exponent)
	}
	return 0
}

// LowBoundary returns the lower boundary of the precision interval for this decimal
// The optional precision parameter specifies the output precision (default 8)
// The caller should validate that outputPrecision is in the range 0-31
func (d Decimal) LowBoundary(ctx context.Context, outputPrecision *int) (Decimal, error) {
	targetPrecision := 8
	if outputPrecision != nil {
		targetPrecision = *outputPrecision
	}

	originalPrecision := d.Precision()

	// Use the apd context from the Go context, but ensure sufficient precision for calculation
	baseCtx := apdContext(ctx)
	// LowBoundary needs to round down (floor), so create a copy with modified rounding
	calcCtx := *baseCtx
	calcCtx.Rounding = apd.RoundFloor
	// Ensure we have enough precision for intermediate calculations
	// We need at least originalPrecision + targetPrecision + 2 for safe calculation
	minPrecision := uint32(originalPrecision + targetPrecision + 2)
	if calcCtx.Precision < minPrecision {
		calcCtx.Precision = minPrecision
	}

	// Calculate the half-width of the precision interval: 0.5 * 10^(-originalPrecision)
	var halfWidth apd.Decimal
	halfWidth.SetFinite(5, -1-int32(originalPrecision)) // 5 * 10^(-1-originalPrecision) = 0.5 * 10^(-originalPrecision)

	var result apd.Decimal
	var err error

	// For both positive and negative numbers, subtract the half-width
	_, err = calcCtx.Sub(&result, d.Value, &halfWidth)
	if err != nil {
		return Decimal{}, err
	}

	// Format to target precision using Quantize with the exponent
	var formatted apd.Decimal
	_, err = calcCtx.Quantize(&formatted, &result, -int32(targetPrecision))
	if err != nil {
		return Decimal{}, err
	}

	return Decimal{Value: &formatted}, nil
}

// HighBoundary returns the upper boundary of the precision interval for this decimal
// The optional precision parameter specifies the output precision (default 8)
// The caller should validate that outputPrecision is in the range 0-31
func (d Decimal) HighBoundary(ctx context.Context, outputPrecision *int) (Decimal, error) {
	targetPrecision := 8
	if outputPrecision != nil {
		targetPrecision = *outputPrecision
	}

	originalPrecision := d.Precision()

	// Use the apd context from the Go context, but ensure sufficient precision for calculation
	baseCtx := apdContext(ctx)
	// HighBoundary needs to round up (ceiling), so create a copy with modified rounding
	calcCtx := *baseCtx
	calcCtx.Rounding = apd.RoundCeiling
	// Ensure we have enough precision for intermediate calculations
	// We need at least originalPrecision + targetPrecision + 2 for safe calculation
	minPrecision := uint32(originalPrecision + targetPrecision + 2)
	if calcCtx.Precision < minPrecision {
		calcCtx.Precision = minPrecision
	}

	// Calculate the half-width of the precision interval: 0.5 * 10^(-originalPrecision)
	var halfWidth apd.Decimal
	halfWidth.SetFinite(5, -1-int32(originalPrecision)) // 5 * 10^(-1-originalPrecision) = 0.5 * 10^(-originalPrecision)

	var result apd.Decimal
	var err error

	// For both positive and negative numbers, add the half-width
	_, err = calcCtx.Add(&result, d.Value, &halfWidth)
	if err != nil {
		return Decimal{}, err
	}

	// Format to target precision using Quantize with the exponent
	var formatted apd.Decimal
	_, err = calcCtx.Quantize(&formatted, &result, -int32(targetPrecision))
	if err != nil {
		return Decimal{}, err
	}

	return Decimal{Value: &formatted}, nil
}

func (d Decimal) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "Decimal",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (d Decimal) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Value)
}
func (d Decimal) String() string {
	return d.Value.Text('f')
}

type Date struct {
	defaultConversionError[Date]
	Value     time.Time
	Precision DatePrecision
}

type DatePrecision string

const (
	DatePrecisionYear  DatePrecision = "year"
	DatePrecisionMonth DatePrecision = "month"
	DatePrecisionFull  DatePrecision = "full"
)

const (
	maxMillisecondNanoseconds = int(time.Millisecond * 999)
	minTimeZoneOffsetHours    = -12
	maxTimeZoneOffsetHours    = 14
	maxDateDigits             = 8
	maxDateTimeDigits         = 17
	maxTimeDigits             = 9
)

func datePrecisionOrder(p DatePrecision) int {
	switch p {
	case DatePrecisionYear:
		return 0
	case DatePrecisionMonth:
		return 1
	default:
		return 2
	}
}

var dateComparisonLevels = []DatePrecision{
	DatePrecisionYear,
	DatePrecisionMonth,
	DatePrecisionFull,
}

func hasDatePrecisionLevel(current, level DatePrecision) bool {
	return datePrecisionOrder(current) >= datePrecisionOrder(level)
}

func compareDatesAtLevel(a, b time.Time, level DatePrecision) int {
	switch level {
	case DatePrecisionYear:
		return compareInts(a.Year(), b.Year())
	case DatePrecisionMonth:
		if cmp := compareInts(a.Year(), b.Year()); cmp != 0 {
			return cmp
		}
		return compareInts(int(a.Month()), int(b.Month()))
	default:
		if cmp := compareInts(a.Year(), b.Year()); cmp != 0 {
			return cmp
		}
		if cmp := compareInts(int(a.Month()), int(b.Month())); cmp != 0 {
			return cmp
		}
		return compareInts(a.Day(), b.Day())
	}
}

func datePrecisionToDateTimePrecision(p DatePrecision) DateTimePrecision {
	switch p {
	case DatePrecisionYear:
		return DateTimePrecisionYear
	case DatePrecisionMonth:
		return DateTimePrecisionMonth
	default:
		return DateTimePrecisionDay
	}
}

func (d Date) Children(name ...string) Collection {
	return nil
}
func (d Date) PrecisionDigits() int {
	return dateDigitsForPrecision(d.Precision)
}
func (d Date) ToString(explicit bool) (v String, ok bool, err error) {
	return String(d.String()), true, nil
}
func (d Date) ToDate(explicit bool) (v Date, ok bool, err error) {
	return d, true, nil
}
func (d Date) ToDateTime(explicit bool) (v DateTime, ok bool, err error) {
	return DateTime{
		Value:       d.Value,
		Precision:   datePrecisionToDateTimePrecision(d.Precision),
		HasTimeZone: false,
	}, true, nil
}
func (d Date) Equal(other Element) (eq bool, ok bool) {
	o, ok, err := other.ToDate(false)
	if err == nil && ok {
		cmp, cmpOK, err := d.Cmp(o)
		if err == nil {
			return cmp == 0, cmpOK
		}
	}
	if delegatesToDateTime(other) || isStringish(other) {
		return other.Equal(d)
	}
	return false, true
}
func (d Date) Equivalent(other Element) bool {
	o, ok, err := other.ToDate(false)
	if err == nil && ok {
		cmp, cmpOK, err := d.Cmp(o)
		if err == nil && cmpOK {
			return cmp == 0
		}
		return false
	}
	if delegatesToDateTime(other) || isStringish(other) {
		return other.Equivalent(d)
	}
	return false
}
func (d Date) Cmp(other Element) (cmp int, ok bool, err error) {
	o, ok, err := other.ToDate(false)
	if err != nil || !ok {
		return 0, false, fmt.Errorf("can not compare Date to %T, left: %v right: %v", other, d, other)
	}
	right := o.Value.In(d.Value.Location())
	for _, level := range dateComparisonLevels {
		leftHas := hasDatePrecisionLevel(d.Precision, level)
		rightHas := hasDatePrecisionLevel(o.Precision, level)

		if !leftHas && !rightHas {
			break
		}
		if leftHas && rightHas {
			cmp = compareDatesAtLevel(d.Value, right, level)
			if cmp != 0 {
				return cmp, true, nil
			}
			continue
		}
		return 0, false, nil
	}
	return 0, true, nil
}

// Add implements date arithmetic for Date values
func (d Date) Add(ctx context.Context, other Element) (Element, error) {
	// Check for empty date
	if d.Value.IsZero() {
		return nil, fmt.Errorf("cannot perform arithmetic on empty date")
	}

	q, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not add Date with %T: %v + %v", other, d, other)
	}

	unit := normalizeTimeUnit(string(q.Unit))
	if !isTimeUnit(unit) {
		return nil, fmt.Errorf("invalid time unit: %v", q.Unit)
	}

	// Get the value for the quantity, ignoring decimal portion for calendar durations
	var integ, frac apd.Decimal
	q.Value.Value.Modf(&integ, &frac)
	value, err := integ.Int64()
	if err != nil {
		return nil, fmt.Errorf("invalid quantity value for date arithmetic: %v", err)
	}

	// Perform calendar-based arithmetic (negate the value for subtraction)
	var result time.Time
	switch unit {
	case UnitYear:
		result = d.Value.AddDate(int(value), 0, 0)
		// If the month and day of the date or time value is not a valid date in the resulting year,
		// the last day of the calendar month is used.
		if result.Day() < d.Value.Day() {
			result = result.AddDate(0, 0, -result.Day())
		}
	case UnitMonth:
		years, months := value/12, value%12
		result = d.Value.AddDate(int(years), int(months), 0)
		// If the resulting date is not a valid date in the resulting year,
		// the last day of the resulting calendar month is used.
		if result.Day() < d.Value.Day() {
			result = result.AddDate(0, 0, -result.Day())
		}
	case UnitWeek:
		result = d.Value.AddDate(0, 0, int(value)*7)
	case UnitDay:
		result = d.Value.AddDate(0, 0, int(value))
	default:
		return nil, fmt.Errorf("invalid time unit for Date: %v", q.Unit)
	}

	return Date{Value: result, Precision: d.Precision}, nil
}

// Subtract implements date arithmetic for Date values
func (d Date) Subtract(ctx context.Context, other Element) (Element, error) {
	// Check for empty date
	if d.Value.IsZero() {
		return nil, fmt.Errorf("cannot perform arithmetic on empty date")
	}

	q, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not subtract from Date with %T: %v - %v", other, d, other)
	}

	unit := normalizeTimeUnit(string(q.Unit))
	if !isTimeUnit(unit) {
		return nil, fmt.Errorf("invalid time unit: %v", q.Unit)
	}

	// Get the value for the quantity, ignoring decimal portion for calendar durations
	var integ, frac apd.Decimal
	q.Value.Value.Modf(&integ, &frac)
	value, err := integ.Int64()
	if err != nil {
		return nil, fmt.Errorf("invalid quantity value for date arithmetic: %v", err)
	}

	// Perform calendar-based arithmetic (negate the value for subtraction)
	var result time.Time
	switch unit {
	case UnitYear:
		result = d.Value.AddDate(int(-value), 0, 0)
		// If the month and day of the date or time value is not a valid date in the resulting year,
		// the last day of the calendar month is used.
		if result.Day() < d.Value.Day() {
			result = result.AddDate(0, 0, -result.Day())
		}
	case UnitMonth:
		years, months := value/12, value%12
		result = d.Value.AddDate(int(-years), int(-months), 0)
		// If the resulting date is not a valid date in the resulting year,
		// the last day of the resulting calendar month is used.
		if result.Day() < d.Value.Day() {
			result = result.AddDate(0, 0, -result.Day())
		}
	case UnitWeek:
		result = d.Value.AddDate(0, 0, int(-value)*7)
	case UnitDay:
		result = d.Value.AddDate(0, 0, int(-value))
	default:
		return nil, fmt.Errorf("invalid time unit for Date: %v", q.Unit)
	}

	return Date{Value: result, Precision: d.Precision}, nil
}

func (d Date) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "Date",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (d Date) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}
func (d Date) String() string {
	var ds string
	switch d.Precision {
	case DatePrecisionYear:
		ds = d.Value.Format(DateFormatOnlyYear)
	case DatePrecisionMonth:
		ds = d.Value.Format(DateFormatUpToMonth)
	default:
		ds = d.Value.Format(DateFormatFull)
	}
	return fmt.Sprintf("%s", ds)
}

func (d Date) LowBoundary(precisionDigits *int) (Date, bool) {
	digits := maxDateDigits
	if precisionDigits != nil {
		digits = *precisionDigits
	}
	if digits < 0 {
		return Date{}, false
	}
	return buildDateBoundary(d, digits, false)
}

func (d Date) HighBoundary(precisionDigits *int) (Date, bool) {
	digits := maxDateDigits
	if precisionDigits != nil {
		digits = *precisionDigits
	}
	if digits < 0 {
		return Date{}, false
	}
	return buildDateBoundary(d, digits, true)
}

func dateDigitsForPrecision(p DatePrecision) int {
	switch p {
	case DatePrecisionYear:
		return 4
	case DatePrecisionMonth:
		return 6
	default:
		return 8
	}
}

func datePrecisionFromDigits(d int) (DatePrecision, bool) {
	switch d {
	case 4:
		return DatePrecisionYear, true
	case 6:
		return DatePrecisionMonth, true
	case 8:
		return DatePrecisionFull, true
	default:
		return "", false
	}
}

func dateRangeEndpoints(d Date) (time.Time, time.Time) {
	loc := d.Value.Location()
	year, month, day := d.Value.Date()
	switch d.Precision {
	case DatePrecisionYear:
		start := time.Date(year, time.January, 1, 0, 0, 0, 0, loc)
		end := time.Date(year, time.December, 31, 23, 59, 59, maxMillisecondNanoseconds, loc)
		return start, end
	case DatePrecisionMonth:
		start := time.Date(year, month, 1, 0, 0, 0, 0, loc)
		lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, loc).Day()
		end := time.Date(year, month, lastDay, 23, 59, 59, maxMillisecondNanoseconds, loc)
		return start, end
	default:
		start := time.Date(year, month, day, 0, 0, 0, 0, loc)
		end := time.Date(year, month, day, 23, 59, 59, maxMillisecondNanoseconds, loc)
		return start, end
	}
}

func buildDateFromTime(t time.Time, precision DatePrecision) Date {
	loc := t.Location()
	year, month, day := t.Date()
	switch precision {
	case DatePrecisionYear:
		month = time.January
		day = 1
	case DatePrecisionMonth:
		day = 1
	case DatePrecisionFull:
		// keep specific day
	}
	return Date{
		Value:     time.Date(year, month, day, 0, 0, 0, 0, loc),
		Precision: precision,
	}
}

func buildDateBoundary(value Date, digits int, useUpper bool) (Date, bool) {
	precision, ok := datePrecisionFromDigits(digits)
	if !ok {
		return Date{}, false
	}
	start, end := dateRangeEndpoints(value)
	anchor := start
	if useUpper {
		anchor = end
	}
	return buildDateFromTime(anchor, precision), true
}

type Time struct {
	defaultConversionError[Time]
	Value     time.Time
	Precision TimePrecision
}

type TimePrecision string

const (
	TimePrecisionHour        TimePrecision = "hour"
	TimePrecisionMinute      TimePrecision = "minute"
	TimePrecisionSecond      TimePrecision = "second"
	TimePrecisionMillisecond TimePrecision = "millisecond"
	TimePrecisionFull                      = TimePrecisionMillisecond
)

var timeComparisonLevels = []TimePrecision{
	TimePrecisionHour,
	TimePrecisionMinute,
	TimePrecisionSecond,
}

func hasTimePrecisionLevel(current, level TimePrecision) bool {
	if level == TimePrecisionSecond {
		return timePrecisionOrder(current) >= timePrecisionOrder(TimePrecisionSecond)
	}
	return timePrecisionOrder(current) >= timePrecisionOrder(level)
}

func compareTimesAtLevel(a, b time.Time, level TimePrecision) int {
	switch level {
	case TimePrecisionHour:
		return compareInts(a.Hour(), b.Hour())
	case TimePrecisionMinute:
		if cmp := compareInts(a.Hour(), b.Hour()); cmp != 0 {
			return cmp
		}
		return compareInts(a.Minute(), b.Minute())
	default:
		if cmp := compareInts(a.Hour(), b.Hour()); cmp != 0 {
			return cmp
		}
		if cmp := compareInts(a.Minute(), b.Minute()); cmp != 0 {
			return cmp
		}
		if cmp := compareInts(a.Second(), b.Second()); cmp != 0 {
			return cmp
		}
		aMillis := a.Nanosecond() / int(time.Millisecond)
		bMillis := b.Nanosecond() / int(time.Millisecond)
		return compareInts(aMillis, bMillis)
	}
}

func timePrecisionOrder(p TimePrecision) int {
	switch p {
	case TimePrecisionHour:
		return 0
	case TimePrecisionMinute:
		return 1
	case TimePrecisionSecond:
		return 2
	default:
		return 3
	}
}

func (t Time) Children(name ...string) Collection {
	return nil
}
func (t Time) PrecisionDigits() int {
	return timeDigitsForPrecision(t.Precision)
}

func (t Time) ToString(explicit bool) (v String, ok bool, err error) {
	return String(t.String()), true, nil
}
func (t Time) ToTime(explicit bool) (v Time, ok bool, err error) {
	return t, true, nil
}
func (t Time) Equal(other Element) (eq bool, ok bool) {
	o, ok, err := other.ToTime(false)
	if err == nil && ok {
		cmp, cmpOK, err := t.Cmp(o)
		if err == nil {
			return cmp == 0, cmpOK
		}
	}
	if delegatesToDateTime(other) || isStringish(other) {
		return other.Equal(t)
	}
	return false, true
}
func (t Time) Equivalent(other Element) bool {
	o, ok, err := other.ToTime(false)
	if err == nil && ok {
		cmp, cmpOK, err := t.Cmp(o)
		if err == nil && cmpOK {
			return cmp == 0
		}
		return false
	}
	if delegatesToDateTime(other) || isStringish(other) {
		return other.Equivalent(t)
	}
	return false
}
func (t Time) Cmp(other Element) (cmp int, ok bool, err error) {
	o, ok, err := other.ToTime(false)
	if err != nil || !ok {
		return 0, false, fmt.Errorf("can not compare Time to %T, left: %v right: %v", other, t, other)
	}
	right := o.Value.In(t.Value.Location())
	for _, level := range timeComparisonLevels {
		leftHas := hasTimePrecisionLevel(t.Precision, level)
		rightHas := hasTimePrecisionLevel(o.Precision, level)

		if !leftHas && !rightHas {
			break
		}
		if leftHas && rightHas {
			cmp = compareTimesAtLevel(t.Value, right, level)
			if cmp != 0 {
				return cmp, true, nil
			}
			continue
		}
		return 0, false, nil
	}
	return 0, true, nil
}

// Add implements time arithmetic for Time values
func (t Time) Add(ctx context.Context, other Element) (Element, error) {
	// Check for empty time
	if t.Value.IsZero() {
		return nil, fmt.Errorf("cannot perform arithmetic on empty time")
	}

	q, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not add Time with %T: %v + %v", other, t, other)
	}

	unit := normalizeTimeUnit(string(q.Unit))
	if !isTimeUnit(unit) {
		return nil, fmt.Errorf("invalid time unit: %v", q.Unit)
	}

	// For calendar durations, truncate decimal values
	var integ, frac apd.Decimal
	q.Value.Value.Modf(&integ, &frac)
	value, err := integ.Int64()
	if err != nil {
		return nil, fmt.Errorf("invalid quantity value for date arithmetic: %v", err)
	}

	var result time.Time
	switch unit {
	case UnitHour:
		result = t.Value.Add(time.Duration(value * int64(time.Hour)))
	case UnitMinute:
		result = t.Value.Add(time.Duration(value * int64(time.Minute)))
	case UnitSecond:
		seconds, err := q.Value.Value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid quantity value for datetime arithmetic: %v", err)
		}
		result = t.Value.Add(time.Duration(seconds * float64(time.Second)))
	case UnitMillisecond:
		milliseconds, err := q.Value.Value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid quantity value for datetime arithmetic: %v", err)
		}
		result = t.Value.Add(time.Duration(milliseconds * float64(time.Millisecond)))
	default:
		return nil, fmt.Errorf("invalid time unit for Time: %v", q.Unit)
	}

	// Normalize time to have date component at 0000-01-01
	// Time arithmetic wraps around 24 hours, so extract time-of-day only
	year, month, day := result.Date()
	if year != 0 || month != 1 || day != 1 {
		hour, min, sec := result.Clock()
		nsec := result.Nanosecond()
		result = time.Date(0, 1, 1, hour, min, sec, nsec, result.Location())
	}

	return Time{Value: result, Precision: t.Precision}, nil
}

// Subtract implements time arithmetic for Time values
func (t Time) Subtract(ctx context.Context, other Element) (Element, error) {
	// Check for empty time
	if t.Value.IsZero() {
		return nil, fmt.Errorf("cannot perform arithmetic on empty time")
	}

	q, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not subtract from Time with %T: %v - %v", other, t, other)
	}

	unit := normalizeTimeUnit(string(q.Unit))
	if !isTimeUnit(unit) {
		return nil, fmt.Errorf("invalid time unit: %v", q.Unit)
	}

	// For calendar durations, truncate decimal values
	var integ, frac apd.Decimal
	q.Value.Value.Modf(&integ, &frac)
	value, err := integ.Int64()
	if err != nil {
		return nil, fmt.Errorf("invalid quantity value for date arithmetic: %v", err)
	}

	var result time.Time
	switch unit {
	case UnitHour:
		result = t.Value.Add(time.Duration(-value * int64(time.Hour)))
	case UnitMinute:
		result = t.Value.Add(time.Duration(-value * int64(time.Minute)))
	case UnitSecond:
		seconds, err := q.Value.Value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid quantity value for datetime arithmetic: %v", err)
		}
		result = t.Value.Add(time.Duration(-seconds * float64(time.Second)))
	case UnitMillisecond:
		milliseconds, err := q.Value.Value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid quantity value for datetime arithmetic: %v", err)
		}
		result = t.Value.Add(time.Duration(-milliseconds * float64(time.Millisecond)))
	default:
		return nil, fmt.Errorf("invalid time unit for Time: %v", q.Unit)
	}

	// Normalize time to have date component at 0000-01-01
	// Time arithmetic wraps around 24 hours, so extract time-of-day only
	year, month, day := result.Date()
	if year != 0 || month != 1 || day != 1 {
		hour, min, sec := result.Clock()
		nsec := result.Nanosecond()
		result = time.Date(0, 1, 1, hour, min, sec, nsec, result.Location())
	}

	return Time{Value: result, Precision: t.Precision}, nil
}

func (t Time) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "Time",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}
func (t Time) String() string {
	var ts string
	switch t.Precision {
	case TimePrecisionHour:
		ts = t.Value.Format(TimeFormatOnlyHour)
	case TimePrecisionMinute:
		ts = t.Value.Format(TimeFormatUpToMinute)
	case TimePrecisionSecond:
		ts = t.Value.Format(TimeFormatUpToSecond)
	case TimePrecisionMillisecond:
		ts = t.Value.Format(TimeFormatFull)
	default:
		ts = t.Value.Format(TimeFormatFull)
	}
	return fmt.Sprintf("@T%s", ts)
}

func (t Time) LowBoundary(precisionDigits *int) (Time, bool) {
	digits := maxTimeDigits
	if precisionDigits != nil {
		digits = *precisionDigits
	}
	if digits < 0 {
		return Time{}, false
	}
	return buildTimeBoundary(t, digits, false)
}

func (t Time) HighBoundary(precisionDigits *int) (Time, bool) {
	digits := maxTimeDigits
	if precisionDigits != nil {
		digits = *precisionDigits
	}
	if digits < 0 {
		return Time{}, false
	}
	return buildTimeBoundary(t, digits, true)
}

func timeDigitsForPrecision(p TimePrecision) int {
	switch p {
	case TimePrecisionHour:
		return 2
	case TimePrecisionMinute:
		return 4
	case TimePrecisionSecond:
		return 6
	default:
		return 9
	}
}

func timePrecisionFromDigits(d int) (TimePrecision, bool) {
	switch d {
	case 2:
		return TimePrecisionHour, true
	case 4:
		return TimePrecisionMinute, true
	case 6:
		return TimePrecisionSecond, true
	case 9:
		return TimePrecisionMillisecond, true
	default:
		return "", false
	}
}

func timeRangeEndpoints(t Time) (time.Time, time.Time) {
	loc := t.Value.Location()
	value := t.Value.In(loc)
	hour, min, sec := value.Clock()
	nsec := value.Nanosecond()
	switch t.Precision {
	case TimePrecisionHour:
		start := time.Date(0, 1, 1, hour, 0, 0, 0, loc)
		end := time.Date(0, 1, 1, hour, 59, 59, maxMillisecondNanoseconds, loc)
		return start, end
	case TimePrecisionMinute:
		start := time.Date(0, 1, 1, hour, min, 0, 0, loc)
		end := time.Date(0, 1, 1, hour, min, 59, maxMillisecondNanoseconds, loc)
		return start, end
	case TimePrecisionSecond:
		moment := time.Date(0, 1, 1, hour, min, sec, 0, loc)
		return moment, moment
	default:
		aligned := alignToMillisecond(nsec)
		moment := time.Date(0, 1, 1, hour, min, sec, aligned, loc)
		return moment, moment
	}
}

func buildTimeFromTime(t time.Time, precision TimePrecision) Time {
	loc := t.Location()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()
	switch precision {
	case TimePrecisionHour:
		min, sec, nsec = 0, 0, 0
	case TimePrecisionMinute:
		sec, nsec = 0, 0
	case TimePrecisionSecond:
		nsec = 0
	case TimePrecisionMillisecond:
		nsec = alignToMillisecond(nsec)
	}
	return Time{
		Value:     time.Date(0, 1, 1, hour, min, sec, nsec, loc),
		Precision: precision,
	}
}

func buildTimeBoundary(value Time, digits int, useUpper bool) (Time, bool) {
	precision, ok := timePrecisionFromDigits(digits)
	if !ok {
		return Time{}, false
	}
	start, end := timeRangeEndpoints(value)
	anchor := start
	if useUpper {
		anchor = end
	}
	return buildTimeFromTime(anchor, precision), true
}

func alignToMillisecond(nsec int) int {
	ms := int(time.Millisecond)
	return (nsec / ms) * ms
}

type DateTime struct {
	defaultConversionError[DateTime]
	Value       time.Time
	Precision   DateTimePrecision
	HasTimeZone bool
}

type DateTimePrecision string

const (
	DateTimePrecisionYear        DateTimePrecision = "year"
	DateTimePrecisionMonth       DateTimePrecision = "month"
	DateTimePrecisionDay         DateTimePrecision = "day"
	DateTimePrecisionHour        DateTimePrecision = "hour"
	DateTimePrecisionMinute      DateTimePrecision = "minute"
	DateTimePrecisionSecond      DateTimePrecision = "second"
	DateTimePrecisionMillisecond DateTimePrecision = "millisecond"
	DateTimePrecisionFull                          = DateTimePrecisionMillisecond
)

func (dt DateTime) Children(name ...string) Collection {
	return nil
}
func (dt DateTime) PrecisionDigits() int {
	return dateTimeDigitsForPrecision(dt.Precision)
}

func dateTimePrecisionOrder(p DateTimePrecision) int {
	switch p {
	case DateTimePrecisionYear:
		return 0
	case DateTimePrecisionMonth:
		return 1
	case DateTimePrecisionDay:
		return 2
	case DateTimePrecisionHour:
		return 3
	case DateTimePrecisionMinute:
		return 4
	case DateTimePrecisionSecond:
		return 5
	case DateTimePrecisionMillisecond:
		return 6
	default:
		return 7
	}
}

func compareInts(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

var dateTimeComparisonLevels = []DateTimePrecision{
	DateTimePrecisionYear,
	DateTimePrecisionMonth,
	DateTimePrecisionDay,
	DateTimePrecisionHour,
	DateTimePrecisionMinute,
	DateTimePrecisionSecond,
}

func hasDateTimePrecisionLevel(current, level DateTimePrecision) bool {
	if level == DateTimePrecisionSecond {
		return dateTimePrecisionOrder(current) >= dateTimePrecisionOrder(DateTimePrecisionSecond)
	}
	return dateTimePrecisionOrder(current) >= dateTimePrecisionOrder(level)
}

func compareDateTimesAtLevel(a, b time.Time, level DateTimePrecision) int {
	switch level {
	case DateTimePrecisionYear:
		return compareInts(a.Year(), b.Year())
	case DateTimePrecisionMonth:
		if cmp := compareInts(a.Year(), b.Year()); cmp != 0 {
			return cmp
		}
		return compareInts(int(a.Month()), int(b.Month()))
	case DateTimePrecisionDay:
		if cmp := compareInts(a.Year(), b.Year()); cmp != 0 {
			return cmp
		}
		if cmp := compareInts(int(a.Month()), int(b.Month())); cmp != 0 {
			return cmp
		}
		return compareInts(a.Day(), b.Day())
	case DateTimePrecisionHour:
		return compareInts(a.Hour(), b.Hour())
	case DateTimePrecisionMinute:
		if cmp := compareInts(a.Hour(), b.Hour()); cmp != 0 {
			return cmp
		}
		return compareInts(a.Minute(), b.Minute())
	case DateTimePrecisionSecond:
		if cmp := compareInts(a.Hour(), b.Hour()); cmp != 0 {
			return cmp
		}
		if cmp := compareInts(a.Minute(), b.Minute()); cmp != 0 {
			return cmp
		}
		if cmp := compareInts(a.Second(), b.Second()); cmp != 0 {
			return cmp
		}
		return compareMillisWithinSecond(a, b)
	default:
		return 0
	}
}

func compareMillisWithinSecond(a, b time.Time) int {
	aMillis := a.Nanosecond() / int(time.Millisecond)
	bMillis := b.Nanosecond() / int(time.Millisecond)
	return compareInts(aMillis, bMillis)
}

func (dt DateTime) ToString(explicit bool) (v String, ok bool, err error) {
	return String(dt.String()), true, nil
}
func (dt DateTime) ToDate(explicit bool) (v Date, ok bool, err error) {
	if explicit {
		var precision DatePrecision
		switch dt.Precision {
		case DateTimePrecisionYear, DateTimePrecisionMonth:
			precision = DatePrecision(dt.Precision)
		default:
			precision = DatePrecisionFull
		}
		return Date{
			Value:     dt.Value,
			Precision: precision,
		}, true, nil
	}
	return Date{}, false, implicitConversionError[DateTime, Date](dt)
}
func (dt DateTime) ToDateTime(explicit bool) (v DateTime, ok bool, err error) {
	return dt, true, nil
}
func (dt DateTime) Equal(other Element) (eq bool, ok bool) {
	o, ok, err := other.ToDateTime(false)
	if err == nil && ok {
		cmp, cmpOK, err := dt.Cmp(o)
		if err == nil {
			return cmp == 0, cmpOK
		}
	}
	if isStringish(other) {
		return other.Equal(dt)
	}
	return false, true
}
func (dt DateTime) Equivalent(other Element) bool {
	o, ok, err := other.ToDateTime(false)
	if err == nil && ok {
		cmp, cmpOK, err := dt.Cmp(o)
		if err == nil && cmpOK {
			return cmp == 0
		}
		return false
	}
	if isStringish(other) {
		return other.Equivalent(dt)
	}
	return false
}
func (dt DateTime) Cmp(other Element) (cmp int, ok bool, err error) {
	o, ok, err := other.ToDateTime(false)
	if err != nil || !ok {
		return 0, false, fmt.Errorf("can not compare DateTime to %T, left: %v right: %v", other, dt, other)
	}

	// Per FHIRPath spec, comparisons between DateTime values that both include a time
	// component but differ in timezone awareness are indeterminate.
	leftHasTime := hasDateTimePrecisionLevel(dt.Precision, DateTimePrecisionHour)
	rightHasTime := hasDateTimePrecisionLevel(o.Precision, DateTimePrecisionHour)
	if leftHasTime && rightHasTime && dt.HasTimeZone != o.HasTimeZone {
		return 0, false, nil
	}

	compareTarget := o.Value.In(dt.Value.Location())

	for _, level := range dateTimeComparisonLevels {
		leftHas := hasDateTimePrecisionLevel(dt.Precision, level)
		rightHas := hasDateTimePrecisionLevel(o.Precision, level)

		if !leftHas && !rightHas {
			break
		}
		if leftHas && rightHas {
			cmp = compareDateTimesAtLevel(dt.Value, compareTarget, level)
			if cmp != 0 {
				return cmp, true, nil
			}
			continue
		}
		return 0, false, nil
	}
	return 0, true, nil
}

// Add implements date/time arithmetic for DateTime values
func (dt DateTime) Add(ctx context.Context, other Element) (Element, error) {
	// Check for empty datetime
	if dt.Value.IsZero() {
		return nil, fmt.Errorf("cannot perform arithmetic on empty datetime")
	}

	q, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not add DateTime with %T: %v + %v", other, dt, other)
	}

	unit := normalizeTimeUnit(string(q.Unit))
	if !isTimeUnit(unit) {
		return nil, fmt.Errorf("invalid time unit: %v", q.Unit)
	}

	// For calendar durations, truncate decimal values
	var integ, frac apd.Decimal
	q.Value.Value.Modf(&integ, &frac)
	value, err := integ.Int64()
	if err != nil {
		return nil, fmt.Errorf("invalid quantity value for date arithmetic: %v", err)
	}

	var result time.Time
	switch unit {
	case UnitYear:
		result = dt.Value.AddDate(int(value), 0, 0)
		// If the month and day of the date or time value is not a valid date in the resulting year,
		// the last day of the calendar month is used.
		if result.Day() < dt.Value.Day() {
			result = result.AddDate(0, 0, -result.Day())
		}
	case UnitMonth:
		years, months := value/12, value%12
		result = dt.Value.AddDate(int(years), int(months), 0)
		// If the resulting date is not a valid date in the resulting year,
		// the last day of the resulting calendar month is used.
		if result.Day() < dt.Value.Day() {
			result = result.AddDate(0, 0, -result.Day())
		}
	case UnitWeek:
		result = dt.Value.AddDate(0, 0, int(value)*7)
	case UnitDay:
		result = dt.Value.AddDate(0, 0, int(value))
	case UnitHour:
		result = dt.Value.Add(time.Duration(value * int64(time.Hour)))
	case UnitMinute:
		result = dt.Value.Add(time.Duration(value * int64(time.Minute)))
	case UnitSecond:
		seconds, err := q.Value.Value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid quantity value for datetime arithmetic: %v", err)
		}
		result = dt.Value.Add(time.Duration(seconds * float64(time.Second)))
	case UnitMillisecond:
		milliseconds, err := q.Value.Value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid quantity value for datetime arithmetic: %v", err)
		}
		result = dt.Value.Add(time.Duration(milliseconds * float64(time.Millisecond)))
	}

	return DateTime{Value: result, Precision: dt.Precision, HasTimeZone: dt.HasTimeZone}, nil
}

// Subtract implements date/time arithmetic for DateTime values
func (dt DateTime) Subtract(ctx context.Context, other Element) (Element, error) {
	// Check for empty datetime
	if dt.Value.IsZero() {
		return nil, fmt.Errorf("cannot perform arithmetic on empty datetime")
	}

	q, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not subtract from DateTime with %T: %v - %v", other, dt, other)
	}

	unit := normalizeTimeUnit(string(q.Unit))
	if !isTimeUnit(unit) {
		return nil, fmt.Errorf("invalid time unit: %v", q.Unit)
	}

	// For calendar durations, truncate decimal values
	var integ, frac apd.Decimal
	q.Value.Value.Modf(&integ, &frac)
	value, err := integ.Int64()
	if err != nil {
		return nil, fmt.Errorf("invalid quantity value for date arithmetic: %v", err)
	}

	var result time.Time
	switch unit {
	case UnitYear:
		result = dt.Value.AddDate(int(-value), 0, 0)
		// If the month and day of the date or time value is not a valid date in the resulting year,
		// the last day of the calendar month is used.
		if result.Day() < dt.Value.Day() {
			result = result.AddDate(0, 0, -result.Day())
		}
	case UnitMonth:
		years, months := value/12, value%12
		result = dt.Value.AddDate(int(-years), int(-months), 0)
		// If the resulting date is not a valid date in the resulting year,
		// the last day of the resulting calendar month is used.
		if result.Day() < dt.Value.Day() {
			result = result.AddDate(0, 0, -result.Day())
		}
	case UnitWeek:
		result = dt.Value.AddDate(0, 0, int(-value)*7)
	case UnitDay:
		result = dt.Value.Add(time.Duration(-value * int64(time.Hour)))
	case UnitHour:
		result = dt.Value.Add(time.Duration(-value * int64(time.Hour)))
	case UnitMinute:
		result = dt.Value.Add(time.Duration(-value * int64(time.Minute)))
	case UnitSecond:
		seconds, err := q.Value.Value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid quantity value for datetime arithmetic: %v", err)
		}
		result = dt.Value.Add(time.Duration(-seconds * float64(time.Second)))
	case UnitMillisecond:
		milliseconds, err := q.Value.Value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid quantity value for datetime arithmetic: %v", err)
		}
		result = dt.Value.Add(time.Duration(-milliseconds * float64(time.Millisecond)))
	}

	return DateTime{Value: result, Precision: dt.Precision, HasTimeZone: dt.HasTimeZone}, nil
}

func (dt DateTime) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "DateTime",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (d DateTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}
func (dt DateTime) String() string {
	var ds, ts string
	switch dt.Precision {
	case DateTimePrecisionYear:
		ds = dt.Value.Format(DateFormatOnlyYear)
	case DateTimePrecisionMonth:
		ds = dt.Value.Format(DateFormatUpToMonth)
	case DateTimePrecisionDay:
		ds = dt.Value.Format(DateFormatFull)
	case DateTimePrecisionHour:
		ds = dt.Value.Format(DateFormatFull)
		ts = dt.Value.Format(TimeFormatOnlyHourTZ)
	case DateTimePrecisionMinute:
		ds = dt.Value.Format(DateFormatFull)
		ts = dt.Value.Format(TimeFormatUpToMinuteTZ)
	case DateTimePrecisionSecond:
		ds = dt.Value.Format(DateFormatFull)
		ts = dt.Value.Format(TimeFormatUpToSecondTZ)
	case DateTimePrecisionMillisecond:
		ds = dt.Value.Format(DateFormatFull)
		ts = dt.Value.Format(TimeFormatFullTZ)
	default:
		ds = dt.Value.Format(DateFormatFull)
		ts = dt.Value.Format(TimeFormatFullTZ)
	}

	return fmt.Sprintf("%sT%s", ds, ts)
}

func (dt DateTime) LowBoundary(precisionDigits *int) (DateTime, bool) {
	digits := maxDateTimeDigits
	if precisionDigits != nil {
		digits = *precisionDigits
	}
	if digits < 0 {
		return DateTime{}, false
	}
	return buildDateTimeBoundary(dt, digits, false)
}

func (dt DateTime) HighBoundary(precisionDigits *int) (DateTime, bool) {
	digits := maxDateTimeDigits
	if precisionDigits != nil {
		digits = *precisionDigits
	}
	if digits < 0 {
		return DateTime{}, false
	}
	return buildDateTimeBoundary(dt, digits, true)
}

func dateTimeDigitsForPrecision(p DateTimePrecision) int {
	switch p {
	case DateTimePrecisionYear:
		return 4
	case DateTimePrecisionMonth:
		return 6
	case DateTimePrecisionDay:
		return 8
	case DateTimePrecisionHour:
		return 10
	case DateTimePrecisionMinute:
		return 12
	case DateTimePrecisionSecond:
		return 14
	default:
		return 17
	}
}

func dateTimePrecisionFromDigits(d int) (DateTimePrecision, bool) {
	switch d {
	case 4:
		return DateTimePrecisionYear, true
	case 6:
		return DateTimePrecisionMonth, true
	case 8:
		return DateTimePrecisionDay, true
	case 10:
		return DateTimePrecisionHour, true
	case 12:
		return DateTimePrecisionMinute, true
	case 14:
		return DateTimePrecisionSecond, true
	case 17:
		return DateTimePrecisionMillisecond, true
	default:
		return "", false
	}
}

func dateTimeRangeEndpoints(dt DateTime) (time.Time, time.Time) {
	loc := dt.Value.Location()
	value := dt.Value.In(loc)
	year, month, day := value.Date()
	hour, min, sec := value.Clock()
	nsec := value.Nanosecond()
	switch dt.Precision {
	case DateTimePrecisionYear:
		start := time.Date(year, time.January, 1, 0, 0, 0, 0, loc)
		end := time.Date(year, time.December, 31, 23, 59, 59, maxMillisecondNanoseconds, loc)
		return start, end
	case DateTimePrecisionMonth:
		start := time.Date(year, month, 1, 0, 0, 0, 0, loc)
		lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, loc).Day()
		end := time.Date(year, month, lastDay, 23, 59, 59, maxMillisecondNanoseconds, loc)
		return start, end
	case DateTimePrecisionDay:
		start := time.Date(year, month, day, 0, 0, 0, 0, loc)
		end := time.Date(year, month, day, 23, 59, 59, maxMillisecondNanoseconds, loc)
		return start, end
	case DateTimePrecisionHour:
		start := time.Date(year, month, day, hour, 0, 0, 0, loc)
		end := time.Date(year, month, day, hour, 0, 59, maxMillisecondNanoseconds, loc)
		return start, end
	case DateTimePrecisionMinute:
		start := time.Date(year, month, day, hour, min, 0, 0, loc)
		end := time.Date(year, month, day, hour, min, 59, maxMillisecondNanoseconds, loc)
		return start, end
	case DateTimePrecisionSecond:
		moment := time.Date(year, month, day, hour, min, sec, 0, loc)
		return moment, moment
	default:
		aligned := alignToMillisecond(nsec)
		moment := time.Date(year, month, day, hour, min, sec, aligned, loc)
		return moment, moment
	}
}

func buildDateTimeFromTime(t time.Time, precision DateTimePrecision) DateTime {
	loc := t.Location()
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()
	switch precision {
	case DateTimePrecisionYear:
		month = time.January
		day = 1
		hour, min, sec, nsec = 0, 0, 0, 0
	case DateTimePrecisionMonth:
		day = 1
		hour, min, sec, nsec = 0, 0, 0, 0
	case DateTimePrecisionDay:
		hour, min, sec, nsec = 0, 0, 0, 0
	case DateTimePrecisionHour:
		min, sec, nsec = 0, 0, 0
	case DateTimePrecisionMinute:
		sec, nsec = 0, 0
	case DateTimePrecisionSecond:
		nsec = 0
	case DateTimePrecisionMillisecond:
		nsec = alignToMillisecond(nsec)
	}
	return DateTime{
		Value:     time.Date(year, month, day, hour, min, sec, nsec, loc),
		Precision: precision,
	}
}

func buildDateTimeBoundary(value DateTime, digits int, useUpper bool) (DateTime, bool) {
	precision, ok := dateTimePrecisionFromDigits(digits)
	if !ok {
		return DateTime{}, false
	}
	start, end := dateTimeRangeEndpoints(value)
	anchor := start
	if useUpper {
		anchor = end
	}
	// FHIRPath lowBoundary/highBoundary semantics (FHIRPath v3.0.0, Utility Functions)
	// require floating datetimes to represent the range of possible timezone offsets,
	// modeled as +/-14h (low) and +/-12h (high) adjustments at the requested precision.
	if !value.HasTimeZone && includesTimeComponent(precision) {
		offset := maxTimeZoneOffsetHours
		if useUpper {
			offset = minTimeZoneOffsetHours
		}
		adjHour := adjustHourForOffset(anchor.Hour(), offset)
		anchor = time.Date(anchor.Year(), anchor.Month(), anchor.Day(), adjHour, anchor.Minute(), anchor.Second(), anchor.Nanosecond(), anchor.Location())
	}

	result := buildDateTimeFromTime(anchor, precision)
	if !value.HasTimeZone && includesTimeComponent(result.Precision) {
		result.HasTimeZone = true
	} else {
		result.HasTimeZone = value.HasTimeZone
	}
	return result, true
}

func includesTimeComponent(p DateTimePrecision) bool {
	switch p {
	case DateTimePrecisionHour, DateTimePrecisionMinute, DateTimePrecisionSecond, DateTimePrecisionMillisecond:
		return true
	default:
		return false
	}
}

func adjustHourForOffset(hour, offset int) int {
	adj := hour - offset
	adj %= 24
	if adj < 0 {
		adj += 24
	}
	return adj
}

const (
	DateFormatOnlyYear     = "2006"
	DateFormatUpToMonth    = "2006-01"
	DateFormatFull         = "2006-01-02"
	TimeFormatOnlyHour     = "15"
	TimeFormatOnlyHourTZ   = "15Z07:00"
	TimeFormatUpToMinute   = "15:04"
	TimeFormatUpToMinuteTZ = "15:04Z07:00"
	TimeFormatUpToSecond   = "15:04:05"
	TimeFormatUpToSecondTZ = "15:04:05Z07:00"
	TimeFormatFull         = "15:04:05.999999999"
	TimeFormatFullTZ       = "15:04:05.999999999Z07:00"
)

func ParseDate(s string) (Date, error) {
	ds := strings.TrimLeft(s, "@")

	d, err := time.Parse(DateFormatOnlyYear, ds)
	if err == nil {
		return Date{Value: d, Precision: DatePrecisionYear}, nil
	}
	d, err = time.Parse(DateFormatUpToMonth, ds)
	if err == nil {
		return Date{Value: d, Precision: DatePrecisionMonth}, nil
	}
	d, err = time.Parse(DateFormatFull, ds)
	if err == nil {
		return Date{Value: d, Precision: DatePrecisionFull}, nil
	}

	return Date{}, fmt.Errorf("invalid Date format: %s", s)
}

func ParseTime(s string) (Time, error) {
	return parseTime(s, false)
}

func parseTime(s string, withTZ bool) (Time, error) {
	ts := strings.TrimLeft(s, "@T")
	timePart := ts
	if idx := strings.IndexAny(timePart, "Z+-"); idx != -1 {
		timePart = timePart[:idx]
	}
	hasFraction := strings.Contains(timePart, ".")

	t, err := time.Parse(TimeFormatOnlyHour, ts)
	if err == nil {
		return Time{Value: t, Precision: TimePrecisionHour}, nil
	}
	if withTZ {
		t, err = time.Parse(TimeFormatOnlyHourTZ, ts)
		if err == nil {
			return Time{Value: t, Precision: TimePrecisionHour}, nil
		}
	}
	t, err = time.Parse(TimeFormatUpToMinute, ts)
	if err == nil {
		return Time{Value: t, Precision: TimePrecisionMinute}, nil
	}
	if withTZ {
		t, err = time.Parse(TimeFormatUpToMinuteTZ, ts)
		if err == nil {
			return Time{Value: t, Precision: TimePrecisionMinute}, nil
		}
	}
	if !hasFraction {
		t, err = time.Parse(TimeFormatUpToSecond, ts)
		if err == nil {
			return Time{Value: t, Precision: TimePrecisionSecond}, nil
		}
		if withTZ {
			t, err = time.Parse(TimeFormatUpToSecondTZ, ts)
			if err == nil {
				return Time{Value: t, Precision: TimePrecisionSecond}, nil
			}
		}
	}
	t, err = time.Parse(TimeFormatFull, ts)
	if err == nil {
		return Time{Value: t, Precision: TimePrecisionMillisecond}, nil
	}
	if withTZ {
		t, err = time.Parse(TimeFormatFullTZ, ts)
		if err == nil {
			return Time{Value: t, Precision: TimePrecisionMillisecond}, nil
		}
	}

	return Time{}, fmt.Errorf("invalid Date format: %s", s)
}

func ParseDateTime(s string) (DateTime, error) {
	splits := strings.Split(s, "T")

	ds := splits[0]
	d, err := ParseDate(ds)
	if err != nil {
		return DateTime{}, fmt.Errorf("invalid DateTime format (date part): %s", s)
	}

	hasTimeZone := false
	if len(splits) > 1 && splits[1] != "" {
		tsPart := splits[1]
		if idx := strings.IndexAny(tsPart, "Zz"); idx != -1 {
			hasTimeZone = true
		} else if strings.Contains(tsPart, "+") || strings.Contains(tsPart, "-") {
			hasTimeZone = true
		}
	}
	if len(splits) == 1 || splits[1] == "" {
		if d.Precision == DatePrecisionFull {
			return DateTime{Value: d.Value, Precision: DateTimePrecisionDay, HasTimeZone: false}, nil
		}
		return DateTime{Value: d.Value, Precision: DateTimePrecision(d.Precision), HasTimeZone: false}, nil
	}

	ts := splits[1]
	t, err := parseTime(ts, true)
	if err != nil {
		return DateTime{}, fmt.Errorf("invalid DateTime format (time part): %s", s)
	}

	tv := t.Value.In(d.Value.Location())

	dt := d.Value.Add(
		time.Hour*time.Duration(tv.Hour()) +
			time.Minute*time.Duration(tv.Minute()) +
			time.Second*time.Duration(tv.Second()) +
			time.Nanosecond*time.Duration(tv.Nanosecond()),
	)
	return DateTime{Value: dt, Precision: DateTimePrecision(t.Precision), HasTimeZone: hasTimeZone}, nil
}

// Time units for date/time arithmetic
const (
	UnitYear         = "year"
	UnitYears        = "years"
	UnitMonth        = "month"
	UnitMonths       = "months"
	UnitWeek         = "week"
	UnitWeeks        = "weeks"
	UnitDay          = "day"
	UnitDays         = "days"
	UnitHour         = "hour"
	UnitHours        = "hours"
	UnitMinute       = "minute"
	UnitMinutes      = "minutes"
	UnitSecond       = "second"
	UnitSeconds      = "seconds"
	UnitS            = "s"
	UnitMillisecond  = "millisecond"
	UnitMilliseconds = "milliseconds"
	UnitMs           = "ms"
)

// isTimeUnit returns true if the unit is a valid time unit
func isTimeUnit(unit string) bool {
	switch unit {
	case UnitYear, UnitYears,
		UnitMonth, UnitMonths,
		UnitWeek, UnitWeeks,
		UnitDay, UnitDays,
		UnitHour, UnitHours,
		UnitMinute, UnitMinutes,
		UnitSecond, UnitSeconds, UnitS,
		UnitMillisecond, UnitMilliseconds, UnitMs:
		return true
	}
	return false
}

// normalizeTimeUnit returns the canonical form of a time unit
func normalizeTimeUnit(unit string) string {
	// Strip quotes if present (quantities in FHIRPath always have quoted units)
	if len(unit) >= 2 && unit[0] == '\'' && unit[len(unit)-1] == '\'' {
		unit = unit[1 : len(unit)-1]
	}

	switch unit {
	case UnitYear, UnitYears:
		return UnitYear
	case UnitMonth, UnitMonths:
		return UnitMonth
	case UnitWeek, UnitWeeks, "wk":
		return UnitWeek
	case UnitDay, UnitDays, "d":
		return UnitDay
	case UnitHour, UnitHours, "h":
		return UnitHour
	case UnitMinute, UnitMinutes, "min":
		return UnitMinute
	case UnitSecond, UnitSeconds, UnitS:
		return UnitSecond
	case UnitMillisecond, UnitMilliseconds, UnitMs:
		return UnitMillisecond
	}
	return unit
}

type Quantity struct {
	defaultConversionError[Quantity]
	Value Decimal
	Unit  String
}

func (q Quantity) Children(name ...string) Collection {
	return nil
}
func (q Quantity) ToString(explicit bool) (v String, ok bool, err error) {
	return String(q.String()), true, nil
}
func (q Quantity) ToQuantity(explicit bool) (v Quantity, ok bool, err error) {
	return q, true, nil
}
func (q Quantity) Equal(other Element) (eq bool, ok bool) {
	o, ok, err := other.ToQuantity(false)
	if err == nil && ok {
		leftOrigUnit := q.Unit
		rightOrigUnit := o.Unit
		left := q.canonicalizeUnit()
		right := o.canonicalizeUnit()
		if calendarEqualityRestricted(leftOrigUnit, rightOrigUnit, left.Unit) {
			// Per the FHIRPath specification ("Quantity Equality" section),
			// calendar duration quantities (years/months) are incomparable to
			// corresponding definite UCUM durations (e.g. 'a', 'mo'), so the
			// equality operator must return the empty collection.
			return false, false
		}
		converted, convErr := convertQuantityToUnit(nil, right, left.Unit)
		if convErr != nil {
			return false, false
		}
		eq, eqOK := left.Value.Equal(converted.Value)
		return eq && eqOK, true
	}
	if isStringish(other) {
		return other.Equal(q)
	}
	return false, true
}
func (q Quantity) Equivalent(other Element) bool {
	o, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return false
	}

	left := q.canonicalizeUnit()
	right := o.canonicalizeUnit()
	converted, convErr := convertQuantityToUnit(nil, right, left.Unit)
	if convErr != nil {
		return false
	}
	return left.Value.Equivalent(converted.Value)
}
func (q Quantity) Cmp(other Element) (cmp int, ok bool, err error) {
	o, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return 0, false, fmt.Errorf("can not compare Quantity to %T, left: %v right: %v", other, q, other)
	}
	left := q.canonicalizeUnit()
	right := o.canonicalizeUnit()
	converted, convErr := convertQuantityToUnit(nil, right, left.Unit)
	if convErr != nil {
		return 0, false, fmt.Errorf("quantity units do not match, left: %v right: %v", left, right)
	}
	return left.Value.Cmp(converted.Value)
}
func (q Quantity) Multiply(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not multiply Quantity with %T: %v * %v", other, q, other)
	}
	left := q.canonicalizeUnit()
	right := o.canonicalizeUnit()

	value, err := left.Value.Multiply(ctx, right.Value)
	if err != nil {
		return Quantity{}, err
	}

	return Quantity{Value: value.(Decimal), Unit: formatProductUnit(left.Unit, right.Unit)}, nil
}
func (q Quantity) Divide(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not divide Quantity with %T: %v / %v", other, q, other)
	}
	left := q.canonicalizeUnit()
	right := o.canonicalizeUnit()

	value, err := left.Value.Divide(ctx, right.Value)
	if err != nil {
		return Quantity{}, err
	}
	return Quantity{Value: value.(Decimal), Unit: formatDivisionUnit(left.Unit, right.Unit)}, nil
}
func (q Quantity) Add(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not add Quantity and %T: %v + %v", other, q, other)
	}
	left := q.canonicalizeUnit()
	right := o.canonicalizeUnit()

	converted, convErr := convertQuantityToUnit(ctx, right, left.Unit)
	if convErr != nil {
		return Quantity{}, fmt.Errorf("quantity units do not match, left: %v right: %v", left, right)
	}

	var sum apd.Decimal
	_, err = apdContext(ctx).Add(&sum, left.Value.Value, converted.Value.Value)
	if err != nil {
		return Quantity{}, err
	}
	return Quantity{Value: Decimal{Value: &sum}, Unit: left.Unit}, nil
}
func (q Quantity) Subtract(ctx context.Context, other Element) (Element, error) {
	o, ok, err := other.ToQuantity(false)
	if err != nil || !ok {
		return nil, fmt.Errorf("can not subtract %T from Quantity: %v - %v", other, q, other)
	}
	left := q.canonicalizeUnit()
	right := o.canonicalizeUnit()

	converted, convErr := convertQuantityToUnit(ctx, right, left.Unit)
	if convErr != nil {
		return Quantity{}, fmt.Errorf("quantity units do not match, left: %v right: %v", left, right)
	}

	var diff apd.Decimal
	_, err = apdContext(ctx).Sub(&diff, left.Value.Value, converted.Value.Value)
	if err != nil {
		return Quantity{}, err
	}
	return Quantity{Value: Decimal{Value: &diff}, Unit: left.Unit}, nil
}

func (q Quantity) canonicalizeUnit() Quantity {
	q.Unit = canonicalQuantityUnit(q.Unit)
	return q
}

func canonicalQuantityUnit(unit String) String {
	if unit == "" {
		return "1"
	}
	canonical := canonicalUCUMUnit(string(unit))
	if canonical == "" {
		return "1"
	}
	return String(canonical)
}

// calendarEqualityRestricted returns true if the FHIRPath Quantity equality operator
// must treat the operands as non-comparable, yielding the empty collection.
// Reference: FHIRPath specification, "Quantity Equality" section.
func calendarEqualityRestricted(leftOriginal, rightOriginal, canonicalUnit String) bool {
	leftLiteral := isCalendarLiteralUnit(leftOriginal)
	rightLiteral := isCalendarLiteralUnit(rightOriginal)
	if leftLiteral == rightLiteral {
		return false
	}
	return isVariableLengthCalendarUnit(canonicalUnit)
}

func isCalendarLiteralUnit(unit String) bool {
	switch strings.ToLower(string(unit)) {
	case UnitYear, UnitYears, UnitMonth, UnitMonths, UnitWeek, UnitWeeks, UnitDay, UnitDays,
		UnitHour, UnitHours, UnitMinute, UnitMinutes, UnitSecond, UnitSeconds,
		UnitMillisecond, UnitMilliseconds:
		return true
	default:
		return false
	}
}

func isVariableLengthCalendarUnit(unit String) bool {
	switch strings.ToLower(string(unit)) {
	case "a", "mo":
		return true
	default:
		return false
	}
}

func convertQuantityToUnit(ctx context.Context, q Quantity, unit String) (Quantity, error) {
	target := canonicalQuantityUnit(unit)
	q = q.canonicalizeUnit()

	if q.Unit == target {
		return q, nil
	}

	converted, err := convertDecimalUnit(ctx, q.Value.Value, string(q.Unit), string(target))
	if err != nil {
		return Quantity{}, err
	}

	return Quantity{
		Value: Decimal{Value: converted},
		Unit:  target,
	}, nil
}

func formatProductUnit(left, right String) String {
	switch {
	case left == "1":
		return right
	case right == "1":
		return left
	}
	return String(fmt.Sprintf("%s.%s", wrapNumerator(left), wrapNumerator(right)))
}

func formatDivisionUnit(numerator, denominator String) String {
	switch {
	case numerator == denominator:
		return "1"
	case denominator == "1":
		return numerator
	case numerator == "1":
		return String(fmt.Sprintf("1/%s", wrapDenominator(denominator)))
	}
	return String(fmt.Sprintf("%s/%s", wrapNumerator(numerator), wrapDenominator(denominator)))
}

func wrapNumerator(u String) string {
	s := string(u)
	if strings.ContainsRune(s, '/') {
		return fmt.Sprintf("(%s)", s)
	}
	return s
}

func wrapDenominator(u String) string {
	s := string(u)
	if strings.ContainsAny(s, "./") {
		return fmt.Sprintf("(%s)", s)
	}
	return s
}
func (q Quantity) TypeInfo() TypeInfo {
	return SimpleTypeInfo{
		Namespace: "System",
		Name:      "Quantity",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}
func (q Quantity) MarshalJSON() ([]byte, error) {
	return json.Marshal(q.String())
}
func (q Quantity) String() string {
	u := strings.TrimSpace(string(q.Unit))
	if u == "" {
		return q.Value.String()
	}
	display := displayQuantityUnit(q.Unit)
	if isCalendarLiteralUnit(q.Unit) {
		return fmt.Sprintf("%s %s", q.Value.String(), display)
	}
	return fmt.Sprintf("%s '%s'", q.Value.String(), display)
}

func ParseQuantity(s string) (Quantity, error) {
	expr, err := Parse(s)
	if err != nil {
		return Quantity{}, fmt.Errorf("cannot parse quantity '%s': %v", s, err)
	}
	termCtx, ok := expr.tree.(*parser.TermExpressionContext)
	if !ok {
		return Quantity{}, fmt.Errorf("cannot parse quantity '%s'", s)
	}
	literalCtx, ok := termCtx.Term().(*parser.LiteralTermContext)
	if !ok {
		return Quantity{}, fmt.Errorf("cannot parse quantity '%s'", s)
	}

	switch lit := literalCtx.Literal().(type) {
	case *parser.QuantityLiteralContext:
		quantityCtx, ok := lit.Quantity().(*parser.QuantityContext)
		if !ok {
			return Quantity{}, fmt.Errorf("cannot parse quantity '%s'", s)
		}
		v, _, err := apd.NewFromString(quantityCtx.NUMBER().GetText())
		if err != nil {
			return Quantity{}, err
		}

		t := quantityCtx.Unit().GetText()
		u := strings.Trim(t, "'")

		return Quantity{Value: Decimal{Value: v}, Unit: String(u)}, nil
	case *parser.NumberLiteralContext:
		v, _, err := apd.NewFromString(lit.NUMBER().GetText())
		if err != nil {
			return Quantity{}, err
		}

		return Quantity{Value: Decimal{Value: v}, Unit: "1"}, nil
	default:
		return Quantity{}, fmt.Errorf("cannot parse quantity '%s'", s)
	}
}

type defaultConversionError[F any] struct {
}

func (_ defaultConversionError[F]) ToBoolean(explicit bool) (v Boolean, ok bool, err error) {
	return false, false, conversionError[F, Boolean]()
}
func (_ defaultConversionError[F]) ToString(explicit bool) (v String, ok bool, err error) {
	return "", false, conversionError[F, Boolean]()
}
func (_ defaultConversionError[F]) ToInteger(explicit bool) (v Integer, ok bool, err error) {
	return 0, false, conversionError[F, Integer]()
}
func (_ defaultConversionError[F]) ToLong(explicit bool) (v Long, ok bool, err error) {
	return 0, false, conversionError[F, Long]()
}
func (_ defaultConversionError[F]) ToDecimal(explicit bool) (v Decimal, ok bool, err error) {
	return Decimal{}, false, conversionError[F, Decimal]()
}
func (_ defaultConversionError[F]) ToDate(explicit bool) (v Date, ok bool, err error) {
	return Date{}, false, conversionError[F, Date]()
}
func (_ defaultConversionError[F]) ToTime(explicit bool) (v Time, ok bool, err error) {
	return Time{}, false, conversionError[F, Time]()
}
func (_ defaultConversionError[F]) ToDateTime(explicit bool) (v DateTime, ok bool, err error) {
	return DateTime{}, false, conversionError[F, DateTime]()
}
func (_ defaultConversionError[F]) ToQuantity(explicit bool) (v Quantity, ok bool, err error) {
	return Quantity{}, false, conversionError[F, Quantity]()
}

func conversionError[F any, T Element]() error {
	var (
		f F
		t T
	)
	return fmt.Errorf("primitive %v of type %T can not be converted to type %T", f, f, t)
}

func implicitConversionError[F Element, T Element](f F) error {
	var t T
	return fmt.Errorf("primitive %T %v can not be implicitly converted to %T", f, f, t)
}
