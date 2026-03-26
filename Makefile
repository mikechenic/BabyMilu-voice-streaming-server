PYTHON ?= python
FIRESTORE_EMULATOR_HOST ?= localhost:8080
FIRESTORE_PROJECT_ID ?= baby-milu-local
DEVICES ?= 20
SEED ?= 42
RAMP ?= 5
DURATION ?= 60
WS_URL ?= ws://127.0.0.1:8000/xiaozhi/v1/

.PHONY: dev seed_firestore concurrency_test

dev:
	cd main/xiaozhi-server && FIRESTORE_EMULATOR_HOST=$(FIRESTORE_EMULATOR_HOST) $(PYTHON) app.py

seed_firestore:
	FIRESTORE_EMULATOR_HOST=$(FIRESTORE_EMULATOR_HOST) $(PYTHON) tools/seed_firestore.py --devices $(DEVICES) --seed $(SEED) --project-id $(FIRESTORE_PROJECT_ID) --clear-existing

concurrency_test:
	FIRESTORE_EMULATOR_HOST=$(FIRESTORE_EMULATOR_HOST) $(PYTHON) tools/concurrency_test.py --devices $(DEVICES) --seed $(SEED) --ramp $(RAMP) --duration $(DURATION) --project-id $(FIRESTORE_PROJECT_ID) --ws-url "$(WS_URL)"
