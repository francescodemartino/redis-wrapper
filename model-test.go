package redis_wrapper

type Test2 struct {
	Count int
	Name  string
}

type Test1 struct {
	Num   int
	Text  string
	Flag  bool
	Value Test2
}
