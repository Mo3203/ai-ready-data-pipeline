.PHONY: setup check board reset teardown produce orders drivers counts show

setup:
	bash scripts/setup.sh

check:
	python tools/check_environment.py

board:
	bash scripts/run_board.sh

reset:
	bash scripts/reset_data.sh

teardown:
	bash scripts/teardown.sh

produce:
	python pipeline/producer.py

orders:
	python pipeline/orders_to_postgres.py

drivers:
	python pipeline/drivers_to_redis.py

counts:
	python pipeline/spark_windowed_counts.py

show:
	python tools/show_stores.py
	python tools/show_counts.py
