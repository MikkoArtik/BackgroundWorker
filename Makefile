
get-coverage:
	coverage run -m pytest
	coverage report -m
	coverage html
	cd htmlcov && xdg-open index.html
