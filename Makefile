install:
	pip install -r requirements.txt
lint:
	pylint --disable=R,C main.py
run: 
	python main.py 
verify: 
	export GOOGLE_APPLICATION_CREDENTIALS="/home/nameyeh_cbits/app/434-app/nlp-app/my-project-1520296049403-5b9919eaea46.json"
\n 