package datadir_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/OutOfStack/db/internal/datadir"
	"github.com/stretchr/testify/require"
)

func TestDetect(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		files map[string]string
		want  datadir.Kind
	}{
		{"missing directory", nil, datadir.KindNone},
		{"unrelated file", map[string]string{"notes.txt": "DBWAL\x00\x02"}, datadir.KindNone},
		{"WAL segment", map[string]string{"wal-00000000000000000001.log": "DBWAL\x00\x02"}, datadir.KindWAL},
		{"snapshot", map[string]string{"snapshot-00000000000000000001.db": "DBSNP\x00\x02"}, datadir.KindWAL},
		{"tiered segment", map[string]string{"seg-0000000001.data": "DBSEG\x00\x02"}, datadir.KindTiered},
		{"empty matching file", map[string]string{"wal-00000000000000000001.log": ""}, datadir.KindWAL},
		{"invalid matching header", map[string]string{"seg-0000000001.data": "corrupt"}, datadir.KindTiered},
		{"mixed formats", map[string]string{
			"wal-00000000000000000001.log": "DBWAL\x00\x02",
			"seg-0000000001.data":          "DBSEG\x00\x02",
		}, datadir.KindMixed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "data")
			if test.files != nil {
				require.NoError(t, os.Mkdir(dir, 0o750))
				for name, contents := range test.files {
					require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600))
				}
			}

			got, err := datadir.Detect(dir)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
