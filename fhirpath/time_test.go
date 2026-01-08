package fhirpath_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/damedic/fhir-toolbox-go/fhirpath"
)

func TestDateArithmetic(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		date     fhirpath.Date
		quantity fhirpath.Quantity
		wantAdd  fhirpath.Date
		wantSub  fhirpath.Date
		wantErr  bool
	}{
		{
			name: "add/subtract one year",
			date: fhirpath.Date{
				Value:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "year",
			},
			wantAdd: fhirpath.Date{
				Value:     time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
			wantSub: fhirpath.Date{
				Value:     time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
		},
		{
			name: "add/subtract one month with month end adjustment",
			date: fhirpath.Date{
				Value:     time.Date(2020, 1, 31, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "month",
			},
			wantAdd: fhirpath.Date{
				Value:     time.Date(2020, 2, 29, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
			wantSub: fhirpath.Date{
				Value:     time.Date(2019, 12, 31, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
		},
		{
			name: "add/subtract one week",
			date: fhirpath.Date{
				Value:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "week",
			},
			wantAdd: fhirpath.Date{
				Value:     time.Date(2020, 1, 8, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
			wantSub: fhirpath.Date{
				Value:     time.Date(2019, 12, 25, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
		},
		{
			name: "invalid unit",
			date: fhirpath.Date{
				Value:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "hour",
			},
			wantErr: true,
		},
		{
			name: "empty date",
			date: fhirpath.Date{},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "year",
			},
			wantErr: true,
		},
		{
			name: "decimal year",
			date: fhirpath.Date{
				Value:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(15, -1)}, // 1.5 years
				Unit:  "year",
			},
			wantAdd: fhirpath.Date{
				Value:     time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
			wantSub: fhirpath.Date{
				Value:     time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DatePrecisionFull,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test addition
			got, err := tt.date.Add(ctx, tt.quantity)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if got != tt.wantAdd {
				t.Errorf("Add() = %v, want %v", got, tt.wantAdd)
			}

			// Test subtraction
			got, err = tt.date.Subtract(ctx, tt.quantity)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if got != tt.wantSub {
				t.Errorf("Subtract() = %v, want %v", got, tt.wantSub)
			}
		})
	}
}

func TestTimeArithmetic(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		time     fhirpath.Time
		quantity fhirpath.Quantity
		wantAdd  fhirpath.Time
		wantSub  fhirpath.Time
		wantErr  bool
	}{
		{
			name: "add/subtract one hour",
			time: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "hour",
			},
			wantAdd: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 13, 0, 0, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
			wantSub: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 11, 0, 0, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
		},
		{
			name: "add/subtract one minute",
			time: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "minute",
			},
			wantAdd: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 12, 1, 0, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
			wantSub: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 11, 59, 0, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
		},
		{
			name: "add/subtract one second",
			time: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "second",
			},
			wantAdd: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 12, 0, 1, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
			wantSub: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 11, 59, 59, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
		},
		{
			name: "invalid unit",
			time: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "year",
			},
			wantErr: true,
		},
		{
			name: "empty time",
			time: fhirpath.Time{},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "hour",
			},
			wantErr: true,
		},
		{
			name: "millisecond precision",
			time: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1500, 0)},
				Unit:  "millisecond",
			},
			wantAdd: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 12, 0, 1, 500000000, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
			wantSub: fhirpath.Time{
				Value:     time.Date(0, 1, 1, 11, 59, 58, 500000000, time.UTC),
				Precision: fhirpath.TimePrecisionFull,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test addition
			got, err := tt.time.Add(ctx, tt.quantity)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if got != tt.wantAdd {
				t.Errorf("Add() = %v, want %v", got, tt.wantAdd)
			}

			// Test subtraction
			got, err = tt.time.Subtract(ctx, tt.quantity)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if got != tt.wantSub {
				t.Errorf("Subtract() = %v, want %v", got, tt.wantSub)
			}
		})
	}
}

func TestDateBoundaries(t *testing.T) {
	date := fhirpath.Date{
		Value:     time.Date(2014, time.January, 1, 0, 0, 0, 0, time.UTC),
		Precision: fhirpath.DatePrecisionYear,
	}
	monthDigits := 6
	low, ok := date.LowBoundary(&monthDigits)
	if !ok {
		t.Fatalf("expected low boundary for @2014 with precision 6")
	}
	expectedLow := fhirpath.Date{
		Value:     time.Date(2014, time.January, 1, 0, 0, 0, 0, time.UTC),
		Precision: fhirpath.DatePrecisionMonth,
	}
	if low != expectedLow {
		t.Fatalf("unexpected low boundary: got %v want %v", low, expectedLow)
	}

	high, ok := date.HighBoundary(&monthDigits)
	if !ok {
		t.Fatalf("expected high boundary for @2014 with precision 6")
	}
	expectedHigh := fhirpath.Date{
		Value:     time.Date(2014, time.December, 1, 0, 0, 0, 0, time.UTC),
		Precision: fhirpath.DatePrecisionMonth,
	}
	if high != expectedHigh {
		t.Fatalf("unexpected high boundary: got %v want %v", high, expectedHigh)
	}

	invalidDigits := 5
	if _, ok := date.LowBoundary(&invalidDigits); ok {
		t.Fatalf("expected low boundary to fail for invalid precision digits")
	}
}

func TestDateTimeBoundariesPreserveTimeZone(t *testing.T) {
	loc := time.FixedZone("UTC+2", 2*60*60)
	dt := fhirpath.DateTime{
		Value:       time.Date(2014, time.January, 1, 8, 0, 0, 0, loc),
		Precision:   fhirpath.DateTimePrecisionHour,
		HasTimeZone: true,
	}
	digits := 17
	low, ok := dt.LowBoundary(&digits)
	if !ok {
		t.Fatalf("expected datetime low boundary")
	}
	if !low.HasTimeZone {
		t.Fatalf("low boundary should preserve timezone flag")
	}
	if low.Value.Hour() != 8 || low.Value.Minute() != 0 || low.Value.Second() != 0 || low.Value.Nanosecond() != 0 {
		t.Fatalf("unexpected datetime low boundary value: %v", low.Value)
	}

	high, ok := dt.HighBoundary(&digits)
	if !ok {
		t.Fatalf("expected datetime high boundary")
	}
	if !high.HasTimeZone {
		t.Fatalf("high boundary should preserve timezone flag")
	}
	if high.Value.Hour() != 8 || high.Value.Second() != 59 {
		t.Fatalf("unexpected datetime high boundary hour/second: %v", high.Value)
	}
	if high.Value.Nanosecond() != int(time.Millisecond*999) {
		t.Fatalf("unexpected datetime high boundary nanoseconds: %d", high.Value.Nanosecond())
	}

	floating := fhirpath.DateTime{
		Value:       time.Date(2014, time.January, 1, 8, 0, 0, 0, time.UTC),
		Precision:   fhirpath.DateTimePrecisionHour,
		HasTimeZone: false,
	}
	floatingLow, ok := floating.LowBoundary(&digits)
	if !ok {
		t.Fatalf("expected floating datetime low boundary")
	}
	if !floatingLow.HasTimeZone {
		t.Fatalf("floating datetime boundary should mark timezone")
	}
	// Low boundary should be in +14:00 timezone (earliest possible)
	if floatingLow.Value.Day() != 1 || floatingLow.Value.Hour() != 8 {
		t.Fatalf("floating datetime low boundary should preserve hour with +14:00 timezone: %v", floatingLow.Value)
	}
	_, offset := floatingLow.Value.Zone()
	if offset != 14*3600 {
		t.Fatalf("floating datetime low boundary should have +14:00 offset, got %d", offset)
	}

	floatingHigh, ok := floating.HighBoundary(&digits)
	if !ok {
		t.Fatalf("expected floating datetime high boundary")
	}
	if !floatingHigh.HasTimeZone {
		t.Fatalf("floating datetime boundary should mark timezone")
	}
	// High boundary should be in -12:00 timezone (latest possible)
	if floatingHigh.Value.Hour() != 8 {
		t.Fatalf("floating datetime high boundary should preserve hour with -12:00 timezone: %v", floatingHigh.Value)
	}
	_, offset = floatingHigh.Value.Zone()
	if offset != -12*3600 {
		t.Fatalf("floating datetime high boundary should have -12:00 offset, got %d", offset)
	}
}

func TestTimeBoundaries(t *testing.T) {
	tm := fhirpath.Time{
		Value:     time.Date(0, 1, 1, 10, 30, 0, 0, time.UTC),
		Precision: fhirpath.TimePrecisionMinute,
	}
	digits := 9
	low, ok := tm.LowBoundary(&digits)
	if !ok {
		t.Fatalf("expected time low boundary")
	}
	if low.Value.Second() != 0 || low.Value.Nanosecond() != 0 {
		t.Fatalf("unexpected time low boundary value: %v", low.Value)
	}
	high, ok := tm.HighBoundary(&digits)
	if !ok {
		t.Fatalf("expected time high boundary")
	}
	if high.Value.Second() != 59 || high.Value.Nanosecond() != int(time.Millisecond*999) {
		t.Fatalf("unexpected time high boundary value: %v", high.Value)
	}
}

func TestDateTimeArithmetic(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		datetime fhirpath.DateTime
		quantity fhirpath.Quantity
		wantAdd  fhirpath.DateTime
		wantSub  fhirpath.DateTime
		wantErr  bool
	}{
		{
			name: "add/subtract one year",
			datetime: fhirpath.DateTime{
				Value:     time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "year",
			},
			wantAdd: fhirpath.DateTime{
				Value:     time.Date(2021, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
			wantSub: fhirpath.DateTime{
				Value:     time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
		},
		{
			name: "add/subtract one hour",
			datetime: fhirpath.DateTime{
				Value:     time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "hour",
			},
			wantAdd: fhirpath.DateTime{
				Value:     time.Date(2020, 1, 1, 13, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
			wantSub: fhirpath.DateTime{
				Value:     time.Date(2020, 1, 1, 11, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
		},
		{
			name: "partial date - year only",
			datetime: fhirpath.DateTime{
				Value:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionYear,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "year",
			},
			wantAdd: fhirpath.DateTime{
				Value:     time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionYear,
			},
			wantSub: fhirpath.DateTime{
				Value:     time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionYear,
			},
		},
		{
			name: "month end adjustment",
			datetime: fhirpath.DateTime{
				Value:     time.Date(2020, 1, 31, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "month",
			},
			wantAdd: fhirpath.DateTime{
				Value:     time.Date(2020, 2, 29, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
			wantSub: fhirpath.DateTime{
				Value:     time.Date(2019, 12, 31, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
		},
		{
			name:     "empty datetime",
			datetime: fhirpath.DateTime{},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1, 0)},
				Unit:  "year",
			},
			wantErr: true,
		},
		{
			name: "millisecond precision",
			datetime: fhirpath.DateTime{
				Value:     time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
			quantity: fhirpath.Quantity{
				Value: fhirpath.Decimal{Value: apd.New(1500, 0)},
				Unit:  "millisecond",
			},
			wantAdd: fhirpath.DateTime{
				Value:     time.Date(2020, 1, 1, 12, 0, 1, 500000000, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
			wantSub: fhirpath.DateTime{
				Value:     time.Date(2020, 1, 1, 11, 59, 58, 500000000, time.UTC),
				Precision: fhirpath.DateTimePrecisionFull,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test addition
			got, err := tt.datetime.Add(ctx, tt.quantity)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if got != tt.wantAdd {
				t.Errorf("Add() = %v, want %v", got, tt.wantAdd)
			}

			// Test subtraction
			got, err = tt.datetime.Subtract(ctx, tt.quantity)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if got != tt.wantSub {
				t.Errorf("Subtract() = %v, want %v", got, tt.wantSub)
			}
		})
	}
}

// TestDateTimeToStringPreservesTimezone tests that toString() preserves the original timezone
// from the parsed datetime string. This is a regression test for GitHub issue #9.
func TestDateTimeToStringPreservesTimezone(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "morning with +11:00",
			input:    "@2024-01-15T06:30:00.000+11:00",
			expected: "2024-01-15T06:30:00.000+11:00",
		},
		{
			name:     "afternoon with +11:00",
			input:    "@2024-01-15T16:30:00.000+11:00",
			expected: "2024-01-15T16:30:00.000+11:00",
		},
		{
			name:     "negative timezone",
			input:    "@2024-01-15T06:30:00.000-05:00",
			expected: "2024-01-15T06:30:00.000-05:00",
		},
		{
			name:     "UTC with Z",
			input:    "@2024-01-15T06:30:00.000Z",
			expected: "2024-01-15T06:30:00.000Z",
		},
		{
			name:     "floating datetime no TZ",
			input:    "@2024-01-15T06:30:00.000",
			expected: "2024-01-15T06:30:00.000",
		},
		{
			name:     "UTC with explicit +00:00",
			input:    "@2024-01-15T06:30:00.000+00:00",
			expected: "2024-01-15T06:30:00.000Z", // Go normalizes +00:00 to Z
		},
		{
			name:     "fractional timezone +05:30",
			input:    "@2024-01-15T06:30:00.000+05:30",
			expected: "2024-01-15T06:30:00.000+05:30",
		},
		{
			name:     "date-only DateTime without T",
			input:    "@2024-01-15",
			expected: "2024-01-15",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt, err := fhirpath.ParseDateTime(tt.input)
			if err != nil {
				t.Fatalf("ParseDateTime(%q) failed: %v", tt.input, err)
			}

			got := dt.String()
			if got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}
