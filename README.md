# Data Science Midterm Project

## Project/Goals
The goal of this project is to discover if coorelations exist between house prices and other features in the provided dataset. Then, if a coorelation exists, to build a model that can predit house prices on those features.  
## Process
### Data Importing
- We developed a function that iterated through the folders and gave us an output dataframe (!raw_df.pkl) that we could use to begin exploring the data.
- We exploded our tags into columns and then cleaned up the names. 
- We added initial cleaning steps in this function to drop any rows that did not have an address, longitutde, and latitude. 
### Data Cleaning
- We looked at the missing data for locations and developed an api call that we could use inside our missing data function to add coordinates to rows missing them based on address. 
- We updated the types of columns to align with our future modeling and made sure that column types were consistent.
### Additional data for analysis
- We decided we wanted to see if the proximity to bus stops impacted home value. 
- We developed an api that took the coordinates of each house and located bus stops within 750 meters. The count was added to the dataframe as 'busstops'.
### EDA
- We took the cleaned data and ran visualizations on the set to identify outliers, skew, and any initial coorelations. 
- We chose not to remove any data from the frame until we got to model selection because outliers needed to be dealt with on a case-by-case basis instead of globally. 
### Questions We Wanted Answers To
- We established the following questions were what we were going to explore:
    - Is there a coorelation between the number of bus stops near a house and its value? 
    - Does the state a house is located in have an impact on the value? 
    - Are there any rooms in a house that have an impact on the value of the house? Does the square footage play a role? 
    - If a listing has more tags, does that impact the value of the house? 
### Model Selection
- We created subsets of the data highlighting the features we need to answer the question we were working on. 
- We ran the data through multiple models to try and find the best fit. Once we found the best fit we moved on to the next step. 
- If there was no fit we left that question at that stage, concluding that no coorelation exists. 
### Hyperparameter Tuning
- We used GridSearchCV or Random Search CV to find the best parameters for the model we selected. 
- We used stacking to try to create a more robust model. 

## Results
### Question 1: Number of tags

### Question 2: Bus stops near the home
There is no coorelation between the number of bus stops and the home value. We looked at the basic linear regression chart and none was present. We then binned the values to create buckets to see if that impacted the coorelation and it made little difference. This question always had model scores in the negatives so we left this question at model selection. 
### Question 3: State the home is in

### Question 4: Impact of rooms/square footage

## Challenges 
(discuss challenges you faced in the project)

## Future Goals
(what would you do if you had more time?)
