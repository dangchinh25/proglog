CONFIG_PATH=${HOME}/.proglog

.PHONY: init
gencert:
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca
	
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server
	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: test
test: 
	go test -race ./...

.PHONE: compile
compile:
	protoc api/v1/*.proto --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative --proto_path=.