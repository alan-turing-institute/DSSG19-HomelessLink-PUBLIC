# Improving Systems for Connecting Rough Sleepers to Services Through Prioritisation of Alerts

## Declaration of Ethical Use

The purpose of this Data Science for Social Good (DSSG) project carried out with our partner Homeless Link was to positively impact the lives of vulnerable people sleeping rough in the UK by improving the partner’s processes aiming at connecting them with local service providers. The use of this work for any other targeted purpose including the surveillance or harm of rough sleepers and other communities is considered to be a violation of the licensed use of this project and solution. The rights and wishes of those this work hopes to serve were our priority throughout this process and this must remain the focus in any work done moving forward.

## The Policy Problem

Homelessness is a critical problem in the UK and many countries around the world. According to housing charity [Shelter's report](https://bit.ly/2KXvSD0), 320,000 people were homeless as of 2018, meaning they are individuals who do not have security in their means of shelter. Among the homelessness population, there exists a vulnerable subset of individuals sleeping on the streets, known as rough sleepers. In 2018, over 4,700 rough sleepers were counted in England's homelessness census according to Homeless Link's [rough sleeper report](https://bit.ly/31MkEYO).

Homelessness presents numerous risks, from health issues to social factors and more. According to Crisis (a homelessness non-profit), people sleeping on the street are almost 17 times more likely to have been victims of violence and more than one in three people sleeping rough have been physically assaulted whilst homeless ([Crisis]( https://www.crisis.org.uk/ending-homelessness/about-homelessness))

The government is investing 1.2 billion pounds to help in tackling homelessness ([BBC](https://www.bbc.co.uk/news/education-46289259)). This further highlights the necessity for a data-driven, effective and scalable solution that connects rough sleepers to services and shelters to lift them out of such vulnerable conditions.

## The Organisational Challenge

Homeless Link is a UK charity and membership organisation with a vision to end homelessness in the UK. Homeless Link has built a platform called StreetLink that enables any member of the public to connect people sleeping rough with local services that support them through provision of food, shelter, etc. If one is concerned about someone sleeping rough, StreetLink has provided means to send an alert via phone, mobile app or web containing details such as location, appearance, time seen sleeping, etc. These details are then passed to StreetLink for review. If the alert has sufficient and quality information, the alert is dispatched as a referral to the outreach teams of relevant local service providers. Outreach teams consist of case workers who connect with rough sleepers, carry out needs assessments, and determine the best services to make a positive impact on rough sleepers' lives.

While this system is effective at leveraging the public to connect with rough sleepers, StreetLink is heavily vulnerable to bottlenecks in their system. When the weather is especially harsh, StreetLink will receive a huge influx of alerts that overwhelms their system. For example, StreetLink received thousands of alerts over a few days in February 2018 during a particularly bad patch of storms and weather known as the Beast from the East. It is important that the StreetLink staff review all alerts in a timely manner because time is of the essence when weather conditions are perilous. More generally, some alerts can be immediately referred or discarded when reviewed by volunteers whilst others take significant time and attempts to attain more information from the person who made the referral, which again costs time that could be used to find more rough sleepers through a greater volume of high quality referrals.

## The Machine Learning Problem

*Everything was built in conda using python3.6, see requirements.txt for more.*

### Objective

The objective of our work was to build a model that accurately predicts whether a rough sleeper will be found in the next `train_label_span` days. We assigned each alert a label of 1 if it resulted in a rough sleeper being found and 0 if it did not, leaving alerts where this cannot be determined with a NULL label. Our goal was to prioritise alerts based upon a score assigned to them by machine learning models following training on these outcomes. This prioritised list will then be supplied to StreetLink so that their volunteers can review alerts predicted to be of high quality more quickly; increasing the chance that rough sleepers can successfully be reached (high quality alerts are those which include information likely to be indicative of the subject actually *being* a rough sleeper as well as whether they can be found).

### Unit of Analysis + Temporal Cross Validation

Predictions are made at the alert-level (e.g. determining whether an alert on January 1st, 2019 will lead to a rough sleeper being found in the next X days). We used data from 2017 to 2019 for this analysis, and used a temporal cross-validation approach to train models to ensure that the final selected models could generalise to new data (e.g. incoming alerts in the future post-training). This temporal cross-validation approach splits the data into chunks and trains each model on increasing windows of data; the performance between intervals informs how often a model should be retrained to give consistent performance.

For each model, we define the cohort for training and testing in an experiment YAML file as alerts that arrive in a particular region (or the entirety of the UK) before a given "as of date" (a date determining the split between train and test data, based upon the temporal configuration that is provided in the experiment). Therefore, alerts outside of the region of interest or those that occurred after the "as of date"(s) would not be included in a particular cohort.

### Database Schema and Data Sources

The bulk of our data was provided by Homeless Link; other data was gathered using Dark Sky's API.

In order to replicate our database structure and interaction with the python scripts, a `.env` file should be created in the top-level directory containing database credentials. The format should be as follows:

```
POSTGRES_DBNAME=<DBNAME>
POSTGRES_USER=<USERNAME>
POSTGRES_PASSWORD=<PASSWORD>
POSTGRES_HOST=<HOSTNAME>
POSTGRES_PORT=<PORT>
```

After this file has been created, the `setup_db.sh` should be executed to install relevant PSQL extensions and create the schemas and features required for modelling.

### Feature Generation

We have generated several features capturing and augmenting different dimensions of an alert and the corresponding local authority in order to help our models make good predictions as to whether an alert will result in finding a rough sleeper. The feature groups are illustrated below:

* **Demographics**: Age, Gender, etc.
* **Time**: The dates and times at which alerts were raised and resolved, as well as lag and circular temporal features on a weekly / monthly / yearly level.
* **Textual**:  Topic modelling on location and verb entities in location and appearance free text descriptions; word counts of the activities of rough sleepers.
* **Local authority level**: How many alerts has a local authority received in the past X days (X = 7 days, 28 days, 60 days, 180 days)? How many referrals have been made in the past X days? How many rough sleepers have been found in the past X days?
* **Spatial**: Latitude + longitude
* **Spatio-temporal**: Count of alerts Y distance (Y = 50 meters, 500 meters, 1000 meters) away in past X days, count of referrals Y distance away in past X days, count of rough sleepers being found X distance away in past X days and so on
* **Weather**: Temperature, precipitation, wind-speed, all at different aggregations (London-only)

We unfortunately could not include more nuanced information such as the number of attempts made to connect with rough sleepers because of data limitations and in the interest of maintaining anonymity in the data.

### Experiment Configuration

Experiment configuration YAML files can be found in this repository in the `experiments` folder. The pipeline built for training many model configurations was designed to accept different cohorts and model types / parameters via these files. A valid experiment file is structured as follows:

1. A unique ID is supplied at the top of the file
2. Temporal cross-validation configuration should be specified (note that all dates should be provided in YYYY-MM-DD format and all periods as an integer followed by a letter specifying the unit, e.g. 1w for 1 week), consisting of:
  - `data_starts` is the date at which the dataset begins
  - `data_ends` is the most recent alert / entity present in the data
  - `train_label_span` is the quantity of days referenced above (in the Objective section); this determines the range in which an outcome is allowed to occur and it still be deemed a positive outcome
  - `test_label_span` same as above but for the test data, we kept these at the same value throughout our experiments and believe they should perhaps be even less than the 7 days we settled on for our final models
  - `train_span` is how far back from each "as of date" the training data should span, limited by `data_starts`
  - `train_frequency` defines how often within the `train_span` an "as of date" should be generated
  - `test_span` is similar but governs the amount of data to include after each "as_of_date"
  - `test_frequency` defines how often within the `test_span` an "as of date" should be generated
  - `model_update_frequency` determines how often a temporal fold should be made and a new model trained, each time this occurs a new set of "as of dates" are generated for training and testing with the adjustment that `data_starts` is effectively pushed back by `model_update_frequency`
  All of these factors incite the sequential generation of a series of "as of dates" back from the start of the data, abiding by the rules set out in the configuration above.
3. The cohort section of the YAML file allows for the naming of a table which is created as a subset of all of the alerts in the dataset, such that only alerts that occurred on one of the "as of dates" are included. Further filters for region etc. can be included at this stage.
4. The label section also includes a query to augment the cohort with labels for training. The query itself could be edited for fine grain control of the label but in general utilises the "as of dates" and `test/train_label_span` to assign 1's, 0's or NULLs to each alert in the cohort.
The positive and negative definition lists can be populated to decide which outcomes belong in which side of the label; anything not included will be assigned a NULL label.
5. Similarly, the features section includes two lists to decide which SQL-generated and python-generated features to include during training. The query performs a simple join with the (presumably) existing features table.
6. Finally, the classifiers section is used and must be of the following form:
  - The full path of an algorithm / learner type must be supplied (e.g. `sklearn.ensemble.RandomForestClassifier`)
  - Parameters can then be defined as lists belonging to each parameter relevant to that algorithm, and the experiment will try each combination at runtime.
  See the experiments provided for examples of these configurations.

## Setup and Running the Pipeline

In order to install the necessary packages for the pipeline to run, use the `requirements.txt` provided. This can be done easily using `pip install -r requirements.txt`. For the LDA and other text-based features there is also a requirement for the installation of the appropriate language model, which can be done by executing `python -m spacy download en_core_web_sm`.

Scripts relevant to NLP processing are in the `utils` directory. The topic modelling process is divided into two steps: 
1. Extract entities from text (get_location_entities.py and get_verb_entities.py). Before running these scripts, make sure to change the input query and output path. The entities will be stored as a pickle object.
2. Run [Latent Dirichlet Allocoation (LDA)](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation) on entities. The LDA modelling is called via `run.py` along with the `-v` and `-l` flags to create the required verb and location entity pickle files which can then be used to augment cohorts with features during training. Note that paths to the generated pickles should be updated accoringly in `run.py` for the latest ones to be used.

Following the creation of a valid experiment, the pipeline can be executed by navigating to `src` using `run.py` accompanied by the `-r` flag and the `-e <experiment name>` flag. The pipeline will then update you on its progress using fancy `tqdm` bars (!!!) and write to `logging/pipeline_debug.log` during execution. Each model configuration and boolean indicators of the features used are stored in the database alongside all of the model's predictions and evaluations via precision and recall at all possible *k* for that fold (after ordering by the model's predicted scores). This allows for evaluation to be carried out in parallel with the experiment if the user wishes.

### Evaluation and Plotting

Use `plot.py -h` located in the `src` folder to discover our automated plotting framework. These plots are meant for large-scale, initial evaluation of models before more in-depth analysis can take place. There is facility for plots to be made by model / parameter configuration / algorithm across an experiment, to show precision / recall at all *k* or at specific *k* over time / folds (for an explanation of the concept of taking metrics at *k* see [here](https://medium.com/@m_n_malaeb/recall-and-precision-at-k-for-recommender-systems-618483226c54)). We also investigated biases in our models using statistically-backed cross-tabs and bias metrics during model selection to ensure demographics were treated fairly. We would recommend the [Aequitas](https://github.com/dssg/aequitas) project (maintained by DSSG) for this, though we had to do it ourselves due to the secure environment we were working with. Most of the scripts used for bias and fairness evaluation were written in SQL and can be found in the `sql/post_modelling` directory.

If you wish to plot the feature importance of a set of models with the same set of parameters across all the temporal folds, use `plot_average_model_feature_importance.py`. Make sure to input a valid experiment id and parameter sets. If you wish to plot feature importance of a specific model, use `plot_feature_importance_single_model.py`. This script allows for the visualisation of each feature individually or in more high level groupings.

## Results

We selected the best models for two different labels (one focussing on person found vs. person not found and one focussing on referral vs. no referral) from different learners such as Random Forest, Extra Trees, KNN, Decision Trees, etc. Our models return a score related to the selected label for that experiment for each alert. We then sort this list of alerts issued on a given day based on the order of the score and then set different list-lengths (i.e. *k*) in which *k* represents the resource constraints of Homeless Link. Anything above the *k* threshold will be predicted as "rough sleeper found" and anything below the *k* threshold will be predicted as "rough sleeper not found". We then calculate precision and recall at these different *k* thresholds to assess models across different volumes of alerts and temporal folds.

We compared our models against the baseline, which is calculated via HomelessLink's current process. The baseline is determined from the percentage of rough sleepers found amongst all referrals. The baseline assumes that referrals being passed to outreach teams have sufficient information for outreach teams to find rough sleepers. The validity of this assumption is unknown given that Homeless Link's process is such that a referral made me made even in situations of imperfect information. This is because a false positive (StreetLink believes a rough sleeper will be found but there is no rough sleeper) is preferable than a false negative (StreetLink believes there is no rough sleeper, but there is in fact a rough sleeper).

Our final best model's were configurations of the Random Forest and AdaBoost algorithms, which performed very similarly and can be generated using the `rf_all` and `ada_all` experiments in the `experiments` directory. Both of these models were trained on all of the data, though we did experiment with separating the London and Non-London data and training separate models.

See our culminating poster [here](DataFestPoster.pdf).

## Next Steps

With more time, we hope to include more granular weather data covering the entirety of the UK rather than just for London. Furthermore, we would also bring in Open Street Maps data to develop more features relating to an alert's proximity to points of interest (banks, shops, bridges etc.). There is further work in NLP and topic modelling that could be achieved without the constraints of the secure environment we worked in. Work was started on a potential de-duplication algorithm for the top few alerts our models choose; see `dedup_cosineSim.py` for a potential basis to continue this work.

## Coverage of the Work

We were excited to have our work catch the attention of numerous journalists and news sources throughout the fellowship. Thank you to those who took the time to interview or talk with us, and for providing more coverage to Homeless Link and their fantastic cause:

* [Reuters: Tech Tool Aims to Stop Bottlenecks in Reaching UK Homeless](https://uk.reuters.com/article/britain-homelessness-technology/tech-tool-aims-to-stop-bottlenecks-in-reaching-uk-homeless-idUKL5N25O4VD)
* [BBC Radio 4: Interview with Katie Prescott](https://twitter.com/kprescott/status/1169207076079448065?s=21)
* [Sifted: How Machine Learning is Being Used to Tackle Homelessness](https://sifted.eu/articles/tackling-homelessness-machine-learning/?utm_source=twitter.com&utm_medium=social&utm_campaign=hootsuite-ai-homelessness)

## Authors

This project was carried out as part of the Data Science for Social Good (DSSG) 2019 Fellowship.

*DSSG Fellows:*
* Lucia (Lushi) Chen
* Zoe Kimpel
* Austin Nguyen
* Harrison Wilde

*Project Manager:*
* Joshua Sidgwick

*Technical Mentor:*
* Adolfo de Unanue

## Acknowledgements

We would like to acknowledge the work of our partner and mentors. Thank you Gareth Thomas and Davide Veronese at Homeless Link for sharing with us your domain expertise in the homelessness sector, organizational context, and support throughout this process. Thank you for Adolfo de Unanue for hours spent shepherding us us on this technical journey. Thank you for Joshua Sidgwick for your support, warmth, and unrelenting confidence in us. Thank you Rayid Ghani for your leadership and guidance throughout the program.
