from faker import Faker  # type: ignore
import datetime
import csv
import sys
import json

faker = Faker()
client = "CompanyABC"

start = datetime.date(2025, 1, 1)
end = datetime.date(2025, 12, 31)

prefix = "camp_0"
campaign_ids = [
    prefix + str(faker.unique.random_int(min=100, max=500)) for _ in range(100)
]


def generate_row():
    return {
        "client": client,
        "date": faker.date_between(start_date=start,
                                   end_date=end).strftime("%Y-%m-%d"),
        "channel": faker.random_element(
            elements=("Instagram", "Facebook", "TikTok", "YouTube", "Google")
        ),
        "impressions": faker.random_int(min=1005, max=5005),
        "clicks": faker.random_int(min=1005, max=5005),
        "conversions": faker.random_int(min=1005, max=5005),
        "cost_per_click": float(
            faker.pydecimal(left_digits=1, right_digits=2,
                            min_value=1, max_value=5)
        ),
        # 'campaign_id': faker.random_element(elements=campaign_ids),
        'spend_usd': float(faker.pydecimal(
            left_digits=3,
            right_digits=2,
            min_value=10,
            max_value=999
        )),
        'event': faker.random_element(
            elements=(
                'sign_up',
                'page_view',
                'purchase',
                'checkout',
                'add_to_cart'
            ))
    }


if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0]) if args else 10

    dataset = [generate_row() for _ in range(total_count)]

    # generate csv files
    csv_f = f"ad_spend_{client}_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
    with open(csv_f, "w", newline="") as file:
        fieldnames = ["client", "date", "channel", "campaign_id", "spend_usd"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(dataset)

    # generate .txt files
    txt_f = f"clickstreams_{client}_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"
    with open(txt_f, "w") as file:
        for row in dataset:
            pipe = " | ".join(f"{key}: {value}" for key, value in row.items())
            file.write(pipe + "\n")

    # generate .json files
    json_f = f"performance_{client}_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
    with open(json_f, "w") as file:
        json.dump(dataset, file, indent=4)
