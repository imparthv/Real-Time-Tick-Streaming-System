Tick System:
- This project implements a real-time tick producer–consumer system using Django, Celery, RabbitMQ, and Binance WebSocket.

Bringing up the stack: 
- Prerequisites:
    - Python 3.10+
    - Django
    - Celery
    - Redis / RabbitMq
    - MySQL
    - Docker & Docker Compose
    - Binance Websocker API

- Starting services:
    - Use : 
        - docker compose --build
        - docker compose up
    - This will start:
        - Django application
        - Celery Worker'
        - RabbitMQ broker
        - Mysql

- Django admin login steps:
    - Migrating all the tables:
        docker compose web exec python manage.py migrate
    - Create a superuser:
        - docker compose web exec python manage.py createsuperuser
        - Add relvant information
    - Access the login panel using and login using superuser credentials
        http://localhost:8000/

- Adding Broker and Scripts:
    - Step 1: Adding Broker:
        1. In Django admin panel go to brokers:
        2. Click on Add Brokers and fill relevant details
    - Step 2: Adding Scripts:
        1. In Django admin panel go to scripts
        2. Click on Add Brokers and fill relevant details
- Running the producer command:
    - Command Syntax: 
    python manage.py run_tick_producer --broker_id=<broker_id>

    - Example:
    docker-compose exec web python manage.py run_tick_producer --broker_id=1

- Verify ticks in DB:
    - Using Django admin:
    1. Go to django admin
    2. Open ticks
    3. Verify data

