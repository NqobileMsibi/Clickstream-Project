import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

EVENT_HUB_NAMESPACE = "clickstream"
EVENT_HUB_CONNECTION_STRING = "EVENT_HUB_CONNECTION_STRING"
EVENTS_PER_SECOND = 5

conf = {
    "bootstrap.servers": "clickstream.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": EVENT_HUB_CONNECTION_STRING,
    "client.id": "clickstream-producer"
}

producer = Producer(conf)


PRODUCT_CATALOG = [
    {
        "product_id": "prod_001",
        "product_name": "Wireless Mouse",
        "category": "electronics",
        "price": 24.99,
    },
    {
        "product_id": "prod_002",
        "product_name": "Mechanical Keyboard",
        "category": "electronics",
        "price": 89.99,
    },
    {
        "product_id": "prod_003",
        "product_name": "Noise Cancelling Headphones",
        "category": "electronics",
        "price": 199.99,
    },
    {
        "product_id": "prod_004",
        "product_name": "Running Shoes",
        "category": "apparel",
        "price": 74.99,
    },
    {
        "product_id": "prod_005",
        "product_name": "Yoga Mat",
        "category": "fitness",
        "price": 29.99,
    },
    {
        "product_id": "prod_006",
        "product_name": "Water Bottle",
        "category": "fitness",
        "price": 14.99,
    },
    {
        "product_id": "prod_007",
        "product_name": "Backpack",
        "category": "accessories",
        "price": 54.99,
    },
    {
        "product_id": "prod_008",
        "product_name": "Smart Watch",
        "category": "electronics",
        "price": 249.99,
    },
    {
        "product_id": "prod_009",
        "product_name": "Desk Lamp",
        "category": "home",
        "price": 39.99,
    },
    {
        "product_id": "prod_010",
        "product_name": "Coffee Maker",
        "category": "home",
        "price": 119.99,
    },
]

SEARCH_TERMS = [
    "laptop",
    "wireless mouse",
    "running shoes",
    "keyboard",
    "headphones",
    "water bottle",
    "smart watch",
    "desk lamp",
    "coffee maker",
    "backpack",
    "yoga mat",
]

COUNTRIES = ["United States", "Canada", "United Kingdom", "Germany",
             "France", "India", "Nigeria", "South Africa", "Brazil", "Mexico"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Safari", "Edge", "Firefox"]
OPERATING_SYSTEMS = ["iOS", "Android", "Windows", "macOS", "Linux"]
REFERRERS = ["direct", "google", "facebook",
             "instagram", "email", "affiliate", "bing"]


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def weighted_choice(items, weights):
    return random.choices(items, weights=weights, k=1)[0]


def get_user_context():
    device_type = weighted_choice(
        ["mobile", "desktop", "tablet"],
        [0.58, 0.34, 0.08]
    )

    if device_type == "mobile":
        os_name = weighted_choice(["iOS", "Android"], [0.48, 0.52])
        browser = weighted_choice(
            ["Chrome", "Safari", "Firefox"], [0.56, 0.38, 0.06])
    elif device_type == "desktop":
        os_name = weighted_choice(
            ["Windows", "macOS", "Linux"], [0.63, 0.29, 0.08])
        browser = weighted_choice(["Chrome", "Edge", "Safari", "Firefox"], [
                                  0.61, 0.18, 0.11, 0.10])
    else:
        os_name = weighted_choice(["iOS", "Android"], [0.65, 0.35])
        browser = weighted_choice(
            ["Safari", "Chrome", "Firefox"], [0.55, 0.40, 0.05])

    return {
        "user_id": f"user_{random.randint(1, 10000)}",
        "session_id": str(uuid.uuid4()),
        "device_type": device_type,
        "browser": browser,
        "os": os_name,
        "country": weighted_choice(
            COUNTRIES,
            [0.45, 0.08, 0.10, 0.07, 0.06, 0.10, 0.03, 0.03, 0.05, 0.03]
        ),
        "referrer": weighted_choice(
            REFERRERS,
            [0.28, 0.32, 0.12, 0.08, 0.10, 0.06, 0.04]
        )
    }


def base_event(context, event_type, event_timestamp=None):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": event_timestamp or utc_now_iso(),
        "user_id": context["user_id"],
        "session_id": context["session_id"],
        "device_type": context["device_type"],
        "browser": context["browser"],
        "os": context["os"],
        "country": context["country"],
        "referrer": context["referrer"],
    }


def page_view_event(context, page_url, event_timestamp):
    event = base_event(context, "page_view", event_timestamp)
    event.update({
        "page_url": page_url,
        "product_id": None,
        "product_name": None,
        "category": None,
        "search_term": None,
        "cart_value": None,
        "order_id": None,
        "revenue": None,
        "error_code": None,
    })
    return event


def search_event(context, search_term, event_timestamp):
    event = base_event(context, "search", event_timestamp)
    event.update({
        "page_url": "/search",
        "product_id": None,
        "product_name": None,
        "category": None,
        "search_term": search_term,
        "cart_value": None,
        "order_id": None,
        "revenue": None,
        "error_code": None,
    })
    return event


def product_view_event(context, product, event_timestamp):
    event = base_event(context, "product_view", event_timestamp)
    event.update({
        "page_url": f"/products/{product['product_id']}",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "search_term": None,
        "cart_value": None,
        "order_id": None,
        "revenue": None,
        "error_code": None,
    })
    return event


def add_to_cart_event(context, product, cart_value, event_timestamp):
    event = base_event(context, "add_to_cart", event_timestamp)
    event.update({
        "page_url": "/cart",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "search_term": None,
        "cart_value": round(cart_value, 2),
        "order_id": None,
        "revenue": None,
        "error_code": None,
    })
    return event


def checkout_start_event(context, cart_value, event_timestamp):
    event = base_event(context, "checkout_start", event_timestamp)
    event.update({
        "page_url": "/checkout",
        "product_id": None,
        "product_name": None,
        "category": None,
        "search_term": None,
        "cart_value": round(cart_value, 2),
        "order_id": None,
        "revenue": None,
        "error_code": None,
    })
    return event


def purchase_event(context, cart_value, product, event_timestamp):
    event = base_event(context, "purchase", event_timestamp)
    event.update({
        "page_url": "/order-confirmation",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "search_term": None,
        "cart_value": round(cart_value, 2),
        "order_id": str(uuid.uuid4()),
        "revenue": round(cart_value, 2),
        "error_code": None,
    })
    return event


def error_event(context, error_code, page_url, event_timestamp):
    event = base_event(context, "error", event_timestamp)
    event.update({
        "page_url": page_url,
        "product_id": None,
        "product_name": None,
        "category": None,
        "search_term": None,
        "cart_value": None,
        "order_id": None,
        "revenue": None,
        "error_code": error_code,
    })
    return event


def session_start_event(context, event_timestamp):
    event = base_event(context, "session_start", event_timestamp)
    event.update({
        "page_url": "/",
        "product_id": None,
        "product_name": None,
        "category": None,
        "search_term": None,
        "cart_value": None,
        "order_id": None,
        "revenue": None,
        "error_code": None,
    })
    return event


def session_end_event(context, event_timestamp):
    event = base_event(context, "session_end", event_timestamp)
    event.update({
        "page_url": None,
        "product_id": None,
        "product_name": None,
        "category": None,
        "search_term": None,
        "cart_value": None,
        "order_id": None,
        "revenue": None,
        "error_code": None,
    })
    return event


def advance_time(current_time, min_seconds=2, max_seconds=25):
    return current_time + timedelta(seconds=random.randint(min_seconds, max_seconds))


def generate_session():
    """
    Generates a full user session.

    Session types:
    - bounce: starts, views one page, ends
    - browse: views pages/products, no cart
    - cart_abandon: adds to cart but does not checkout
    - checkout_abandon: starts checkout but does not purchase
    - purchase: completes purchase
    """

    context = get_user_context()

    session_type = weighted_choice(
        ["bounce", "browse", "cart_abandon", "checkout_abandon", "purchase"],
        [0.25, 0.40, 0.18, 0.10, 0.07]
    )

    current_time = datetime.now(timezone.utc)
    events = []

    events.append(session_start_event(context, current_time.isoformat()))

    current_time = advance_time(current_time, 1, 5)

    landing_page = weighted_choice(
        ["/", "/deals", "/new-arrivals", "/search",
            "/category/electronics", "/category/apparel"],
        [0.45, 0.12, 0.10, 0.13, 0.12, 0.08]
    )
    events.append(page_view_event(
        context, landing_page, current_time.isoformat()))

    if random.random() < 0.03:
        current_time = advance_time(current_time, 1, 3)
        events.append(error_event(context, "HTTP_500",
                      landing_page, current_time.isoformat()))

    if session_type == "bounce":
        current_time = advance_time(current_time, 2, 10)
        events.append(session_end_event(context, current_time.isoformat()))
        return events

    if random.random() < 0.45:
        current_time = advance_time(current_time, 2, 15)
        search_term = random.choice(SEARCH_TERMS)
        events.append(search_event(
            context, search_term, current_time.isoformat()))

    products_viewed = random.randint(1, 6)

    cart = []
    cart_value = 0.0

    for _ in range(products_viewed):
        current_time = advance_time(current_time, 3, 30)
        product = random.choice(PRODUCT_CATALOG)
        events.append(product_view_event(
            context, product, current_time.isoformat()))

        if random.random() < 0.02:
            current_time = advance_time(current_time, 1, 4)
            events.append(
                error_event(
                    context,
                    weighted_choice(["HTTP_404", "HTTP_500", "PAYMENT_TIMEOUT"], [
                                    0.45, 0.35, 0.20]),
                    f"/products/{product['product_id']}",
                    current_time.isoformat()
                )
            )

        should_add_to_cart = False

        if session_type in ["cart_abandon", "checkout_abandon", "purchase"]:
            should_add_to_cart = random.random() < 0.35
        else:
            should_add_to_cart = random.random() < 0.08

        if should_add_to_cart:
            current_time = advance_time(current_time, 2, 20)
            cart.append(product)
            cart_value += product["price"]
            events.append(add_to_cart_event(context, product,
                          cart_value, current_time.isoformat()))

    if session_type == "browse":
        current_time = advance_time(current_time, 5, 30)
        events.append(session_end_event(context, current_time.isoformat()))
        return events

    if len(cart) == 0 and session_type in ["cart_abandon", "checkout_abandon", "purchase"]:
        current_time = advance_time(current_time, 2, 15)
        product = random.choice(PRODUCT_CATALOG)
        cart.append(product)
        cart_value += product["price"]
        events.append(add_to_cart_event(context, product,
                      cart_value, current_time.isoformat()))

    if session_type == "cart_abandon":
        current_time = advance_time(current_time, 10, 120)
        events.append(session_end_event(context, current_time.isoformat()))
        return events

    current_time = advance_time(current_time, 5, 60)
    events.append(checkout_start_event(
        context, cart_value, current_time.isoformat()))

    if session_type == "checkout_abandon":
        if random.random() < 0.15:
            current_time = advance_time(current_time, 3, 15)
            events.append(error_event(context, "PAYMENT_TIMEOUT",
                          "/checkout", current_time.isoformat()))

        current_time = advance_time(current_time, 10, 180)
        events.append(session_end_event(context, current_time.isoformat()))
        return events

    if session_type == "purchase":
        if random.random() < 0.08:
            current_time = advance_time(current_time, 2, 10)
            events.append(error_event(context, "PAYMENT_RETRY",
                          "/checkout", current_time.isoformat()))

        current_time = advance_time(current_time, 5, 45)
        purchased_product = cart[-1] if cart else random.choice(
            PRODUCT_CATALOG)
        events.append(purchase_event(
            context, cart_value, purchased_product, current_time.isoformat()))

        current_time = advance_time(current_time, 2, 20)
        events.append(session_end_event(context, current_time.isoformat()))
        return events

    current_time = advance_time(current_time, 2, 20)
    events.append(session_end_event(context, current_time.isoformat()))
    return events


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered event to topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def send_event(event):
    key = event["user_id"]
    value = json.dumps(event)

    producer.produce(
        topic=EVENT_HUB_NAME,
        key=key,
        value=value,
        callback=delivery_report
    )

    producer.poll(0)


def main():
    print("Starting realistic session clickstream producer...")
    print(f"Event Hub namespace: {EVENT_HUB_NAMESPACE}")
    print(f"Event Hub name: {EVENT_HUB_NAME}")
    print(f"Target events per second: {EVENTS_PER_SECOND}")

    delay = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0.2

    total_events_sent = 0
    total_sessions_sent = 0

    try:
        while True:
            session_events = generate_session()
            total_sessions_sent += 1

            print(
                f"\nGenerated session {total_sessions_sent} "
                f"with {len(session_events)} events. "
                f"Session ID: {session_events[0]['session_id']}"
            )

            for event in session_events:
                send_event(event)
                total_events_sent += 1

                print(
                    f"Sent event #{total_events_sent}: "
                    f"{event['event_type']} | "
                    f"user={event['user_id']} | "
                    f"session={event['session_id']}"
                )

                time.sleep(delay)

            producer.flush()

    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.flush()
        print(f"Total sessions sent: {total_sessions_sent}")
        print(f"Total events sent: {total_events_sent}")


if __name__ == "__main__":
    main()
