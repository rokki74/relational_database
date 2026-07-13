DIR=~/Desktop/gon/real_dbms

all:	dbs

dbs:
	go build $^ -o $(DIR)/$@

check_hex:
	python3 script.py

clean:
	rm -f dbs *.logs
	rm -r $(DIR)/REALDB
