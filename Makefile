MOCHA_OPTS= --check-leaks
REPORTER = dot

test: test-integration

test-integration:
	@NODE_ENV=test node test/integration/runner.js
