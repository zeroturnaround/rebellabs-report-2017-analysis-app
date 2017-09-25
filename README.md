# RebelLabs Developer Productivity Report 2017 analysis app
An analysis app for the data received for the RebelLabs Developer Productivity Report 2017

The blog post explaining this application is also [available](https://zeroturnaround.com/rebellabs/rebellabs-developer-productivity-report-2017-raw-data).

The report with the results of our analysis is [available on RebelLabs](https://zeroturnaround.com/rebellabs/developer-productivity-report-2017-why-do-you-use-java-tools-you-use).


Get Java 8 (we're not sure this app starts on Java 9 yet).
To run the app, clone the repository, then run:
```
./mvnw spring-boot:run
```

A webapp should open, or check [localhost:8080](http://localhost:8080) manually. 

You will see a list of links, clicking on them you'll see some JSON output like:
![](https://user-images.githubusercontent.com/426039/30813593-80ea6588-a216-11e7-846e-e86c31da150d.png)

If you change the SQL queries, you can do your own analysis on the data. To understand what the queries return, unfortunately, you currently need to read the source code. 

If you're just interested in a raw data, check the [csv file with the raw data](github.com).

