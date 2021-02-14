clean-gen:
	rm -rf gen

gen: clean-gen
	mkdir -p gen
	protoc --go_out=. ./pb/kad.proto