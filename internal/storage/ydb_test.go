package storage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestConvertJson(t *testing.T) {
	v := map[interface{}]interface{}{
		"annotations": map[interface{}]interface{}{
			"checksum/config": []uint8{
				0x62, 0x32, 0x64, 0x39, 0x39, 0x34, 0x64, 0x64, 0x38, 0x65, 0x37, 0x35, 0x35, 0x37, 0x63,
				0x62, 0x31, 0x64, 0x32, 0x30, 0x35, 0x63, 0x64, 0x34, 0x32, 0x30, 0x39, 0x30, 0x62, 0x32,
				0x64, 0x65, 0x64, 0x35, 0x38, 0x62, 0x35, 0x32, 0x30, 0x62, 0x32, 0x38, 0x65, 0x32, 0x61,
				0x30, 0x63, 0x34, 0x65, 0x64, 0x39, 0x61, 0x36, 0x62, 0x37, 0x33, 0x61, 0x34, 0x39, 0x38,
				0x39, 0x34, 0x38, 0x66,
			},
			"container_hash": []uint8{
				0x63, 0x72, 0x2e, 0x79, 0x61, 0x6e, 0x64, 0x65, 0x78, 0x2f, 0x63, 0x72, 0x70, 0x6c, 0x37,
				0x69, 0x70, 0x65, 0x75, 0x37, 0x39, 0x6f, 0x73, 0x65, 0x71, 0x68, 0x63, 0x67, 0x6e, 0x32,
				0x2f, 0x66, 0x6c, 0x75, 0x65, 0x6e, 0x74, 0x2d, 0x62, 0x69, 0x74, 0x2d, 0x79, 0x64, 0x62,
				0x40, 0x73, 0x68, 0x61, 0x32, 0x35, 0x36, 0x3a, 0x66, 0x38, 0x31, 0x36, 0x66, 0x30, 0x36,
				0x32, 0x32, 0x61, 0x64, 0x34, 0x37, 0x66, 0x30, 0x30, 0x31, 0x38, 0x39, 0x39, 0x64, 0x65,
				0x63, 0x62, 0x64, 0x65, 0x35, 0x34, 0x63, 0x35, 0x63, 0x32, 0x66, 0x39, 0x38, 0x37, 0x65,
				0x37, 0x32, 0x63, 0x38, 0x62, 0x39, 0x33, 0x64, 0x30, 0x61, 0x65, 0x39, 0x34, 0x33, 0x65,
				0x35, 0x38, 0x66, 0x34, 0x34, 0x61, 0x34, 0x36, 0x63, 0x61, 0x39, 0x61,
			},
			"container_image": []uint8{
				0x63, 0x72, 0x2e, 0x79, 0x61, 0x6e, 0x64, 0x65, 0x78, 0x2f, 0x63, 0x72, 0x70, 0x6c, 0x37,
				0x69, 0x70, 0x65, 0x75, 0x37, 0x39, 0x6f, 0x73, 0x65, 0x71, 0x68, 0x63, 0x67, 0x6e, 0x32,
				0x2f, 0x66, 0x6c, 0x75, 0x65, 0x6e, 0x74, 0x2d, 0x62, 0x69, 0x74, 0x2d, 0x79, 0x64, 0x62,
				0x3a, 0x32, 0x2e, 0x31, 0x2e, 0x38, 0x2d, 0x61, 0x63, 0x66, 0x63, 0x37, 0x38, 0x34, 0x2d,
				0x31,
			},
			"container_name": []uint8{0x66, 0x6c, 0x75, 0x65, 0x6e, 0x74, 0x2d, 0x62, 0x69, 0x74},
			"docker_id": []uint8{
				0x39, 0x61, 0x35, 0x30, 0x64, 0x37, 0x37, 0x35, 0x31, 0x65, 0x33, 0x35, 0x66, 0x36, 0x36,
				0x61, 0x32, 0x34, 0x37, 0x61, 0x63, 0x39, 0x32, 0x32, 0x31, 0x34, 0x30, 0x35, 0x34, 0x33,
				0x33, 0x65, 0x34, 0x61, 0x31, 0x66, 0x62, 0x34, 0x34, 0x61, 0x66, 0x37, 0x66, 0x35, 0x63,
				0x36, 0x34, 0x31, 0x66, 0x63, 0x38, 0x32, 0x34, 0x32, 0x32, 0x62, 0x35, 0x38, 0x34, 0x37,
				0x30, 0x64, 0x62, 0x32,
			},
			"host": []uint8{
				0x6d, 0x61, 0x6e, 0x34, 0x2d, 0x32, 0x38, 0x31, 0x32, 0x2e, 0x73, 0x65, 0x61, 0x72, 0x63,
				0x68, 0x2e, 0x79, 0x61, 0x6e, 0x64, 0x65, 0x78, 0x2e, 0x6e, 0x65, 0x74,
			},
		},
		"labels": map[interface{}]interface{}{
			"app.kubernetes.io/instance": []uint8{
				0x66, 0x6c, 0x75, 0x65, 0x6e, 0x74, 0x2d, 0x62, 0x69, 0x74,
			},
			"app.kubernetes.io/name": []uint8{
				0x66, 0x6c, 0x75, 0x65, 0x6e, 0x74, 0x2d, 0x62, 0x69, 0x74,
			},
			"controller-revision-hash": []uint8{
				0x64, 0x34, 0x64, 0x62, 0x63, 0x64, 0x64, 0x38, 0x37,
			},
			"pod-template-generation": []uint8{0x31, 0x30},
		},
		"namespace_name": []uint8{0x66, 0x6c, 0x75, 0x65, 0x6e, 0x74, 0x2d, 0x62, 0x69, 0x74},
		"pod_id": []uint8{
			0x30, 0x65, 0x62, 0x36, 0x30, 0x61, 0x62, 0x66, 0x2d, 0x64, 0x34, 0x66, 0x33, 0x2d, 0x34, 0x63,
			0x61, 0x38, 0x2d, 0x38, 0x64, 0x33, 0x34, 0x2d, 0x66, 0x66, 0x31, 0x62, 0x33, 0x38, 0x37, 0x39,
			0x61, 0x62, 0x61, 0x31,
		},
		"pod_name": []uint8{
			0x66, 0x6c, 0x75, 0x65, 0x6e, 0x74, 0x2d, 0x62, 0x69, 0x74, 0x2d, 0x64, 0x74, 0x39, 0x39, 0x6e,
		},
	}
	expected := `{"annotations":{"checksum/config":"b2d994dd8e7557cb1d205cd42090b2ded58b520b28e2a0c4ed9a6b73a498948f",` +
		`"container_hash":"cr.yandex/crpl7ipeu79oseqhcgn2/fluent-bit-ydb@sha256:f816f0622ad47f001899decbde54c5` +
		`c2f987e72c8b93d0ae943e58f44a46ca9a","container_image":"cr.yandex/crpl7ipeu79oseqhcgn2/fluent-bit-ydb:` +
		`2.1.8-acfc784-1","container_name":"fluent-bit","docker_id":"9a50d7751e35f66a247ac9221405433e4a1fb44af` +
		`7f5c641fc82422b58470db2","host":"man4-2812.search.yandex.net"},"labels":{"app.kubernetes.io/instance"` +
		`:"fluent-bit","app.kubernetes.io/name":"fluent-bit","controller-revision-hash":"d4dbcdd87","pod-templ` +
		`ate-generation":"10"},"namespace_name":"fluent-bit","pod_id":"0eb60abf-d4f3-4ca8-8d34-ff1b3879aba1","` +
		`pod_name":"fluent-bit-dt99n"}`

	actual, err := json.Marshal(convertByteFieldsToString(v))

	assert.NoError(t, err)
	assert.Equal(t, expected, string(actual))
}

func TestType2TypeOk(t *testing.T) {
	cases := []struct {
		name     string
		column   types.Type
		value    interface{}
		expected types.Value
	}{
		{
			name:     "convert string value to text",
			column:   types.TypeText,
			value:    "some",
			expected: types.TextValue("some"),
		},
		{
			name:     "convert map to json",
			column:   types.TypeJSON,
			value:    map[interface{}]interface{}{"some": 1, "other": "two"},
			expected: types.JSONValue(`{"other":"two","some":1}`),
		},
		{
			name:     "convert string to optional text",
			column:   types.Optional(types.TypeText),
			value:    "some",
			expected: types.NullableTextValue(pointer("some")),
		},
	}

	for _, tc := range cases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			actual, err := type2Type(tc.column, tc.value)

			assert.NoError(t, err)
			assert.Equal(t, actual, tc.expected)
		})
	}
}
