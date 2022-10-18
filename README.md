# Proglog

## Background

- This is a Golang application that follow the instruction from [this book](https://pragprog.com/titles/tjgo/distributed-services-with-go/) to build a distributed log services.

## NOTES
Offset refers to the logical location of the entry in the file. E.g: if we have 2 entry entry0 & entry1, then entry0 would be at offset 0 and entry1 would be at offset1
Pos refers to the actual location of the entry in the file, representing by bytes
E.g: if we have 2 entry entry0 and entry1, each consists of 8bytes. Then entry0 would be at pos=0 and entry1 would be at pos=8 

## TODO
+ Understand gommap
+ Understand enc ?
+ Understand os.Truncate
+ Understand TLS and CA cert
+ Understand Casbin
+ Understand Serf