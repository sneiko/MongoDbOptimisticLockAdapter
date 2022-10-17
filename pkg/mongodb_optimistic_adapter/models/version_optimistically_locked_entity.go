package models

type VersionOptimisticallyLockedEntity[T any] struct {
	Entity  T
	Version int
}

func NewVersionOptimisticallyLockedEntity[T any](entity T, version int) *VersionOptimisticallyLockedEntity[T] {
	vole := new(VersionOptimisticallyLockedEntity[T])
	vole.Entity = entity
	vole.Version = version
	return vole
}
