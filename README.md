# kafka-producer-earthquake

This project streams real-time earthquake data from the USGS API to a Kafka topic and a data file.
It is part of a pipeline that illustrates analyzing live geospatial data.

## Environment and Local Execution

Use the following PowerShell commands from the root project folder to:

1. Create a local project virtual environment.
2. Activate the virtual environment.
3. Upgrade key packages.
4. Install external dependencies.
5. Run all the tests.
6. Deploy the Kafka producer locally. 

Mac/Linux: Try cross-platform PowerShell or adjust the commands as needed for your operating system. 
See requirements.txt for more information. 

```shell
py -3.11 -m venv .venv
.\.venv\Scripts\activate
py -m pip install --upgrade pip setuptools wheel
py -m pip install --upgrade -r requirements.txt
py -m pytest
py -m producers.producer_earthquake
```

## Additional Notes

- Verify Zookeeper service and Kafka broker service is up and running before starting the producer.
- Create a .env file and configure Kafka broker address, topic name, and other settings.
- The producer will run continuously, fetching and streaming earthquake data. Use Ctrl + C to stop the process.

## Deploying to GitHub Codespaces
GitHub Codespaces gives 60 free hours/month.
To avoid burning hours, stop Codespaces when not using.

## Schedule Runs
This service will run 3 times per weekday (i.e., 10 AM, 2 PM, 6 PM).
Each run will go for 20 minutes. 

3 runs/day × 20 minutes/run × 5 days/week × 4 weeks/month
-  3 × 0.33 × 5 × ~4
-  ~20 hours per month (well within free 60 hours)

##  If Using Oracle Free Tier or Others
Stick to free-tier services (like Oracle Free VM).
Schedule runs. 
NEVER leave resources running.
Check usage in the provider’s dashboard to avoid unexpected charges.

## Save Space
To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later. 
Managing Python virtual environments is a valuable skill. 

## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.
