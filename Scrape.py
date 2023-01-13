import stweet as st


def try_tweet_by_id_scrap(twitter_id):
    '''
    给定推文id，爬取相关的推文
    :param twitter_id: 推文的id
    :return: 整个推特返回回来
    '''
    id_task = st.TweetsByIdTask(twitter_id)
    output_json = st.JsonLineFileRawOutput('twitter3.jl')
    result = st.TweetsByIdRunner(tweets_by_id_task=id_task, raw_data_outputs=[output_json]).run()
    return result
