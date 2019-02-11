package nsqd

import (
	"testing"
)

func TestAddRemoveInts(t *testing.T) {
	r := RangeSet{}
	if len(r.Ranges) != 0 {
		t.Errorf("Length of Ranges is not 0")
	}

	r.AddInts(1, 2, 3)
	t.Logf("r.Ranges: %d \n", r.Ranges)

	if r.Ranges[0].Low != 1 || r.Ranges[0].High != 3 {
		t.Errorf("Expected 1-3")
	}

	r.AddInts(6, 7, 8)
	t.Logf("added 6, 7, 8: %d \n", r.Ranges)

	if r.Ranges[1].Low != 6 || r.Ranges[1].High != 8 {
		t.Errorf("Expected 1-3, 6-8")
	}

	r.AddInts(12, 14, 15, 16, 17)
	t.Logf("added 12, 14-17: %d \n", r.Ranges)

	if r.Ranges[2].Low != 12 || r.Ranges[2].High != 12 {
		t.Errorf("Expected 1-3, 6-8, 12-12, 14-17")
	}

	r.RemoveInts(14, 15)
	t.Logf("removed 14, 15: %d \n", r.Ranges)

	if r.Ranges[3].Low != 16 || r.Ranges[3].High != 17 {
		t.Errorf("Expected 1-3, 6-8, 12-12, 16-17")
	}

	r.AddInts(4, 5)
	t.Logf("added 4, 5: %d \n", r.Ranges)

	if r.Ranges[0].Low != 1 || r.Ranges[0].High != 8 {
		t.Errorf("Expected 1-8, 12-12, 16-17")
	}

	r.AddInts(13)
	t.Logf("added 13: %d \n", r.Ranges)

	if r.Ranges[1].Low != 12 || r.Ranges[1].High != 13 {
		t.Errorf("Expected 1-8, 12-13, 16-17")
	}

	r.RemoveInts(12, 13, 14, 15, 16, 17)
	t.Logf("removed 12-17: %d \n", r.Ranges)

	if len(r.Ranges) != 1 {
		t.Errorf("Expected 1-8")
	}

	r.RemoveInts(2, 3)
	t.Logf("removed 2, 3: %d \n", r.Ranges)

	if len(r.Ranges) != 2 && r.Ranges[0].Low != 1 && r.Ranges[0].High != 1 {
		t.Errorf("Expected 1-1, 4-8")
	}
	if r.Ranges[1].Low != 4 && r.Ranges[1].High != 8 {
		t.Errorf("Expected 1-1, 4-8")
	}
}

func TestAddRemoveRanges(t *testing.T) {
	t.Logf("------------------\n")

	r := RangeSet{}
	if len(r.Ranges) != 0 {
		t.Errorf("Length of Ranges is not 0")
	}

	r.AddRange(Range{10, 100})
	t.Logf("r.Ranges: %d \n", r.Ranges)

	if r.Ranges[0].Low != 10 || r.Ranges[0].High != 100 {
		t.Errorf("Expected 10-100")
	}

	r.AddRange(Range{130, 132})
	t.Logf("added 130-132: %d \n", r.Ranges)

	if r.Ranges[1].Low != 130 || r.Ranges[1].High != 132 {
		t.Errorf("Expected 10-100, 130-132")
	}

	r.AddRange(Range{101, 129})
	t.Logf("added 101-129: %d \n", r.Ranges)

	if r.Ranges[0].Low != 10 || r.Ranges[0].High != 132 {
		t.Errorf("Expected 10-132")
	}

	r.RemoveRange(Range{12, 22})
	t.Logf("removed 12-22: %d \n", r.Ranges)

	if r.Ranges[0].Low != 10 || r.Ranges[0].High != 11 {
		t.Errorf("Expected 10-11, 23-132")
	}

	r.AddRange(Range{5, 20})
	t.Logf("added 5-20: %d \n", r.Ranges)

	if r.Ranges[0].Low != 5 || r.Ranges[0].High != 20 {
		t.Errorf("Expected 5-20, 23-132")
	}

	r.AddRange(Range{4, 1000})
	t.Logf("added 4-1000: %d \n", r.Ranges)

	if r.Ranges[0].Low != 4 || r.Ranges[0].High != 1000 {
		t.Errorf("Expected 4-1000")
	}

	r.RemoveRange(Range{400, 500})
	t.Logf("removed 400-500: %d \n", r.Ranges)

	if r.Ranges[0].Low != 4 || r.Ranges[0].High != 399 {
		t.Errorf("Expected 4-399, 501-1000")
	}
	if r.Ranges[1].Low != 501 || r.Ranges[1].High != 1000 {
		t.Errorf("Expected 4-399, 501-1000")
	}

	r.RemoveRange(Range{505, 2000})
	t.Logf("removed 505-2000: %d \n", r.Ranges)

	if r.Ranges[1].Low != 501 && r.Ranges[1].High != 504 {
		t.Errorf("Expected 4-399, 501-504")
	}

	r.AddRange(Range{410, 420})
	t.Logf("added 410-420: %d \n", r.Ranges)

	if r.Ranges[1].Low != 410 || r.Ranges[1].High != 420 {
		t.Errorf("Expected 4-399, 410-420, 501-504")
	}
	if r.Ranges[2].Low != 501 || r.Ranges[2].High != 504 {
		t.Errorf("Expected 4-399, 410-420, 501-504")
	}
}
