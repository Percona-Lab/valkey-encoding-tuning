package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/caio/go-tdigest/v5"
)

type ListNodeType int

const (
	listMaxListpackSize = "list-max-listpack-size"
	listCompressDepth   = "list-compress-depth"
	listDt              = "list"
)
const (
	Unknown ListNodeType = iota
	BySize
	ByElement
)

// since >1 node converts List to quicklist (still function as listpack as head & tail is uncompressed), we don't need total nodeCount
type ListMetrics struct {
	// for obj size or element count distribution, depending on the setting of `list-max-listpack-size` (positive or negative number)
	// for obj size distribution (default), affecting the number of nodes
	// for element count distribution, affecting the number of nodes if `list-max-listpack-size` is positive number
	nodeDivisionType ListNodeType
	tdigest          *tdigest.TDigest
	objCount         int64
	avgNodeCount     int64
	maxNodeCount     int64
	avgObjSize       int64
	maxObjSize       int64
	avgElementCount  int64
	maxElementCount  int64
}

var optimizationLevel = map[string]int{
	"-1": 4 * 1024,
	"-2": 8 * 1024,
	"-3": 16 * 1024,
	"-4": 32 * 1024,
	"-5": 64 * 1024,
}

func makeListMetrics() ListMetrics {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return ListMetrics{tdigest: t}
}

func (v *ValkeyNode) getListNodeDivisionType() {
	if strings.HasPrefix(v.Config[listMaxListpackSize], "-") {
		v.ListMetrics.nodeDivisionType = BySize
	} else {
		v.ListMetrics.nodeDivisionType = ByElement
	}
}

func (v *ValkeyNode) analyzeList() error {
	v.getListNodeDivisionType()

	var cursor uint64
	for {
		entry, err := scan(v.getClient(), listDt, *listKeyPattern, cursor)
		if err != nil {
			return err
		}
		for _, key := range entry.Elements {
			err := v.analyzeListKey(key)
			if err != nil {
				panic(err)
			}
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}

	return nil
}

func (v *ValkeyNode) analyzeListKey(key string) error {
	lm := &v.ListMetrics
	oldCount := lm.objCount
	lm.objCount++

	ctx := context.Background()
	client := v.getClient()
	// count the number of elements
	// whatever the value of `list-compress-depth` is, there will always be at least 2 nodes stored as uncompressed
	// if there are >= 3 nodes and low op/s, then it can be compressed, level 1
	// if there are more nodes, high op/s then can set compression to higher. Leave more nodes uncompressed
	// so need the element count to check if compression is suitable
	count, err := client.Do(
		ctx,
		client.B().Llen().Key(key).Build(),
	).AsInt64()
	if err != nil {
		return err
	}
	// get key size in bytes
	ksize, err := client.Do(
		ctx,
		client.B().MemoryUsage().Key(key).Build(),
	).AsInt64()

	if err != nil {
		return err
	}
	nodeCount, err := estimateListNodeCount(v.Config[listMaxListpackSize], count, ksize)
	if err != nil {
		return err
	}
	isMaxSizeByElementSize := strings.HasPrefix(v.Config[listMaxListpackSize], "-")
	if isMaxSizeByElementSize {
		lm.tdigest.Add(float64(ksize))
	} else {
		lm.tdigest.Add(float64(count))
	}
	if lm.maxNodeCount < nodeCount {
		lm.maxNodeCount = nodeCount
	}
	lm.avgNodeCount = (nodeCount + (lm.avgNodeCount * oldCount)) / lm.objCount

	if lm.maxObjSize < ksize {
		lm.maxObjSize = ksize
	}
	lm.avgObjSize = (ksize + lm.avgObjSize*oldCount) / lm.objCount

	if lm.maxElementCount < count {
		lm.maxElementCount = count
	}
	lm.avgElementCount = (count + lm.avgElementCount*oldCount) / lm.objCount

	return nil
}

func estimateListNodeCount(configValue string, elementCount, objectSize int64) (int64, error) {
	var nodeCount int64
	var elementSum int64
	var maxNodeSize int
	var err error
	// List datatype create new node depending on the number of element per node, or node size
	isMaxSizeByElementSize := strings.HasPrefix(configValue, "-")
	if isMaxSizeByElementSize {
		maxNodeSize = optimizationLevel[configValue]
		elementSum = objectSize
	} else {
		maxNodeSize, err = strconv.Atoi(configValue)
		if err != nil {
			return -1, err
		}
		elementSum = elementCount
	}
	nodeCount = int64(math.Ceil(float64(elementSum) / float64(maxNodeSize)))

	return nodeCount, nil
}

func (v *ValkeyNode) getListDatatypeAnalysis(analysis *Analysis) {
	var sizeDistrType string
	lm := v.ListMetrics
	if lm.nodeDivisionType == BySize {
		sizeDistrType = "element size"
	} else {
		sizeDistrType = "element count"
	}

	analysis.init(v.Address)
	analysis.Config[listMaxListpackSize] = v.Config[listMaxListpackSize]
	analysis.Config[listCompressDepth] = v.Config[listCompressDepth]

	analysis.Metrics[listDt] = map[string]any{
		kObjCount:        lm.objCount,
		kMaxNodeCount:    lm.maxNodeCount,
		kAvgNodeCount:    lm.avgNodeCount,
		kMaxElementCount: lm.maxElementCount,
		kAvgElementCount: lm.avgElementCount,
		kSizeDistrType:   sizeDistrType,
		kDistribution:    quantileDistribution(lm.tdigest),
	}
}
func (lm *ListMetrics) updateListStatistics(node *ListMetrics) {
	if lm.nodeDivisionType != node.nodeDivisionType {
		if lm.nodeDivisionType != Unknown {
			//	do nothing due to incompatible node division type
			fmt.Fprintln(os.Stderr, "target node's listpack config is incompatible with cluster's config")
			return
		}
		// init value
		lm.nodeDivisionType = node.nodeDivisionType
	}
	objCount := (lm.objCount + node.objCount)
	if objCount == 0 {
		return
	}
	lm.avgObjSize = (lm.avgObjSize*lm.objCount + node.avgObjSize*lm.objCount) / objCount
	lm.avgElementCount = (lm.avgElementCount*lm.objCount + node.avgElementCount*node.objCount) / objCount
	lm.objCount = objCount
	if lm.maxNodeCount < node.maxNodeCount {
		lm.maxNodeCount = node.maxNodeCount
	}
	if lm.maxElementCount < node.maxElementCount {
		lm.maxElementCount = node.maxElementCount
	}
	lm.tdigest.Merge(node.tdigest)

}
