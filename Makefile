.PHONY: lint test run-ui demo-youtube

lint:
	python -m ruff .

test:
	python -m pytest

run-ui:
	streamlit run app.py

demo-youtube:
	python -m src.cli youtube --playlist-id PLRkn2QTxcJf5QpWI1J4Vi2AmLJN1QbWl_ --max-results 5 --output data/output/youtube_detail.xlsx --summary-output data/output/youtube_summary.xlsx
