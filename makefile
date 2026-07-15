DIR=~/Desktop/gon/real_dbms

all:	dbs

dbs:
	go build $^ -o $(DIR)/$@

check_hex:
	python3 script.py

clean:
	-mv *.logs *.log $(DIR)/log_archives
	rm -f dbs
	-rm -r $(DIR)/REALDB

