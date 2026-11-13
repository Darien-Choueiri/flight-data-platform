# flight-data-platform

Commands to run the project (linux)
1. docker build -t flight-pipeline .
2. docker run --rm -m 4g -v "$(pwd):/app" flight-pipeline /opt/spark/bin/spark-submit team_project.py
