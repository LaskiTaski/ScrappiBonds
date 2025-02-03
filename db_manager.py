from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from models import engine, BondData

Session = sessionmaker(bind=engine)


def insert_change_table(information_bonds):
    session = Session()
    isin = information_bonds['isin']

    try:
        existing_bond = session.query(BondData).filter_by(isin=isin).first()

        if existing_bond:
            # Обновляем существующую запись
            updated_bond = update_bond(existing_bond, information_bonds)
            session.commit()
            print(f"Данные о бумаге '{isin}' обновленны.")

        else:
            # Создаем новую запись
            new_bond = insert_bond(information_bonds)
            session.add(new_bond)
            session.commit()
            print(f"Данные о бумаге '{isin}' добавленны.")

    except IntegrityError as e:
        handle_db_error(session, e, "Ошибка целостности данных")
    except Exception as e:
        handle_db_error(session, e, "Произошла ошибка")

    finally:
        session.close()


def update_bond(existing_bond, information):
    existing_bond.url = information['url']
    existing_bond.name = information['name']
    existing_bond.quoting = information['quoting']
    existing_bond.repayment = information['repayment']
    existing_bond.market = information['market']
    existing_bond.nominal = information['nominal']
    existing_bond.frequency = information['frequency']
    existing_bond.date = information['date']
    existing_bond.days = information['days']
    existing_bond.code = information['code']
    existing_bond.qualification = information['qualification']
    existing_bond.update_time = information['update_time']
    return existing_bond


def insert_bond(information):
    return BondData(
        url=information['url'],
        name=information['name'],
        quoting=information['quoting'],
        repayment=information['repayment'],
        market=information['market'],
        nominal=information['nominal'],
        frequency=information['frequency'],
        date=information['date'],
        days=information['days'],
        isin=information['isin'],
        code=information['code'],
        qualification=information['qualification'],
        update_time=information['update_time']
    )


def handle_db_error(session, e, message):
    session.rollback()
    print(f"{message}: {e}")
