package main

import "github.com/caio/go-tdigest/v5"

type sizeStats struct {
	tdigest   *tdigest.TDigest
	count     int
	totalSize int
	avgSize   float64
	maxItem   string
	maxSize   int
}

func makeSizeStats() sizeStats {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return sizeStats{tdigest: t}
}

func (s *sizeStats) add(item string, size int) {
	s.tdigest.Add(float64(size))
	s.totalSize += size
	s.count++
	s.avgSize = float64(s.totalSize / s.count)
	if size > s.maxSize {
		s.maxSize = size
		s.maxItem = item
	}
}

func (s *sizeStats) merge(other *sizeStats) {
	totalCount := s.count + other.count
	if totalCount == 0 {
		return
	}
	totalSize := float64(s.count)*s.avgSize + float64(other.count)*other.avgSize
	s.avgSize = totalSize / float64(totalCount)
	s.count = totalCount
	if other.maxSize > s.maxSize {
		s.maxSize = other.maxSize
		s.maxItem = other.maxItem
	}
	s.tdigest.Merge(other.tdigest)
}
