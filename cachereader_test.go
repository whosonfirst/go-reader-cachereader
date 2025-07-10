package cachereader

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/whosonfirst/go-reader/v2"
)

func TestCacheReader(t *testing.T) {

	ctx := context.Background()

	rel_path := "./fixtures"

	abs_path, err := filepath.Abs(rel_path)

	if err != nil {
		t.Fatalf("Failed to derive absolute path, %v", err)
	}

	reader_uri := fmt.Sprintf("fs://%s", abs_path)

	cache_uri := "gocache://"

	cr_uri := fmt.Sprintf("cachereader://?reader=%s&cache=%s", reader_uri, cache_uri)

	r, err := reader.NewReader(ctx, cr_uri)

	if err != nil {
		t.Fatalf("Failed to create new cachereader, %v", err)
	}

	to_test := []string{
		"101736545.geojson",
	}

	for i := 0; i < 2; i++ {

		for _, path := range to_test {

			fh, err := r.Read(ctx, path)

			if err != nil {
				t.Fatalf("Failed to read %s, %v", path, err)
			}

			defer fh.Close()

			_, err = io.Copy(ioutil.Discard, fh)

			if err != nil {
				t.Fatalf("Failed to copy %s, %v", path, err)
			}

			v, _ := GetLastRead(r, path)

			switch i {
			case 0:
				if v != CacheMiss {
					t.Fatalf("Expected cache miss on first read of %s", path)
				}
			default:
				if v != CacheHit {
					t.Fatalf("Expected cache hit after first read of %s", path)
				}
			}

			exists, err := r.Exists(ctx, path)

			if err != nil {
				t.Fatalf("Failed to determine if %s exists, %v", path, err)
			}

			if !exists {
				t.Fatalf("Expected %s to exist", path)
			}
		}

	}
}
