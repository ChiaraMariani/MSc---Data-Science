January 2025 - Project for _Text Mining & Search_ course

The project aims mainly at a multi class classification of about 50000 Tweets from https://ieeexplore.ieee.org/document/9378065 based on the different types of hate speech present (or not) in each tweet. We used different techniques (Bag of Words, GloVe and sentence-BERT) to vectorize/create embeddings of our tweets and then we classified tweets using MLP, Decision Trees and XGBoost.

The secondary aim of the project is to investigate on whether the classes defined in the classification are indeed representative of how hate speech can be characterised or not: to do this we used clustering techniques on each vector based representation of our tweets and then we took a look at each cluster with a topic modelling approach.

Note that in order to execute the notebooks it will be needed to load GloVe embeddings; here is the link to the file download we used https://huggingface.co/stanfordnlp/glove/resolve/main/glove.twitter.27B.zip, from https://github.com/stanfordnlp/GloVe?tab=readme-ov-file
