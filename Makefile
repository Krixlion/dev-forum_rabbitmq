test-setup:
	docker-compose up -d 
	bash healthcheck.sh
	
test-shutdown:
	docker-compose down