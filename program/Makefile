all:genFilePath statFilePath
genFilePath:genFilePath.c
	mpicc -O2 $^ -o $@
statFilePath:statFilePath.c
	mpicc -O2 $^ -o $@
.phony:genFilePath statFilePath
clean:
	rm -rf genFilePath statFilePath

