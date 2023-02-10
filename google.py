from GoogleNews import GoogleNews


news = GoogleNews(lang='en', start='03/22/2020', end='03/24/2020')
print(news.getVersion())
news.search('corona')
for i in range(5):
    ls = news.results(sort=True)
    for item in ls:
        print(item)

