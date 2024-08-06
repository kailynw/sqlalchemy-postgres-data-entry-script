from sqlalchemy import create_engine, Engine
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, Session
from sqlalchemy.orm import mapped_column
import pathlib
import os
import pandas as pd

class Base(DeclarativeBase):
    pass

class CustomerModel(Base):
    __tablename__ = "customers"
    id: Mapped[int] = mapped_column(primary_key=True)
    customer_id: Mapped[str]
    first_name: Mapped[str]
    last_name: Mapped[str]
    company: Mapped[str]
    city: Mapped[str]
    country: Mapped[str]
    phone_1: Mapped[str]
    phone_2: Mapped[str]
    email: Mapped[str]
    subscription: Mapped[str]
    website: Mapped[str]


def insert_csv_data_into_db():
    customer_data_list: list[CustomerModel] = get_csv_customer_data_list()
    engine = get_db_engine()
    with Session(engine) as session:
        session.add_all(customer_data_list)
        session.commit()
        print(f"Added {len(customer_data_list)} customer records")

def get_csv_customer_data_list():
    current_dir = pathlib.Path(__file__).parent.resolve()
    csv_file_path = os.path.join(current_dir, 'etc', 'customers-100.csv')
    df = pd.read_csv(csv_file_path)
    customer_data_list: list[CustomerModel] =[]

    for index, row in df.iterrows():
        customer = CustomerModel(
            customer_id = row['customer_id'],
            first_name= row['first_name'],
            last_name= row['last_name'],
            company= row['company'],
            city= row['city'],
            country= row['country'],
            phone_1= row['phone_1'],
            phone_2= row['phone_2'],
            email= row['email'],
            subscription= row['subscription'],
            website= row['website']
        )
        customer_data_list.append(customer)
    return customer_data_list
     
def get_db_engine() -> Engine:
    connection_str = "postgresql://postgres:williams1@localhost:5432/postgres"
    return create_engine(connection_str)

### Debug function
def get_db_results():
    engine = get_db_engine()
    with Session(engine) as session:
        data = session.query(CustomerModel).all()
        print("DB results: ")
        print(data)


if __name__ =="__main__":
    insert_csv_data_into_db()






