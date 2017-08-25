run:
	python3 main.py

deps:
	pip3 install -r requirements.txt

deplist:
	pip3 freeze > requirements.txt
