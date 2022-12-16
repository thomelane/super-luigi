update-requirements:
	pip-compile --verbose --upgrade --resolver=backtracking requirements.in
	pip-compile --verbose --upgrade --resolver=backtracking dev.requirements.in
	pip-sync --verbose requirements.txt dev.requirements.txt
