import pandas as pd
from faker import Faker

fake = Faker()

df = pd.DataFrame()
for n in range(50):
    # Generate fake article
    title = fake.sentence()
    content = fake.paragraphs(nb=5)
    # Create pandas data frame
    new = pd.DataFrame({'Title': [title], 'Content': ['\n'.join(content)]})
    df = pd.concat([df, new], ignore_index=True)

df.to_csv('fake_news.csv')
# Display data frame
print(df)

