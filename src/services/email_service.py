from mailjet_rest import Client
from src.database import get_database_connections_pool
from src.services import get_entities

api_key = "1d98821252f0e19ba4d9a6f678a8f129"
api_secret ="4b7f29ef7df2f51dc497f3490a008cd3"
mailjet = Client(auth=(api_key, api_secret))



def envio_masivo_db():
    localDB, _ = get_database_connections_pool()
   
    query_clientes = f"""select c.nombre, o.descripcion,c.date_created from cliente c join comunicacion_empresa o on (c.id = o.cliente_id) 
                        where c.date_created between '2022/09/01' and '2022/12/31' and comentario like 'CFDI Mail' order by c.date_created """
    
    #query_clientes = f"""select c.nombre, c.email as descripcion,c.date_created from cliente c 
    #                join cliente_credito l on (c.id = l.cliente_id)
    #              order by c.date_created"""

    #query_clientes = f"""select c.nombre, o.descripcion,c.date_created from cliente c join comunicacion_empresa o on (c.id = o.cliente_id) 
    #                    join cliente_credito l on (l.cliente_id = c.id)
    #                  where tipo = 'MAIL' order by c.date_created """

    clientes = get_entities(localDB,query_clientes)
    for cliente in clientes:
        print(cliente)
        email = cliente['descripcion']
        if email and '@' in email:
            print("Si tiene arroba")
            if " " not in email:
                print("No tiene espacios!!!")
                if email.endswith(".com"):
                    print("Termina com .com !!!!!")
                    enviar_email(email)
                if email.endswith(".mx"):
                    print("Termina com .mx !!!!!")
                    enviar_email(email)

            else:
                print("Tiene espacios en blanco!!!!")
    print(len(clientes))


def envio_masivo():
    with open("src/services/clientes.txt","r") as f:
        lines = f.readlines()
        for line in lines:
            print(line)
            items = line.split(",")
            mail = items[1]
            print(mail)
            enviar_email(mail)
        print(len(lines))
        



def enviar_email(mail):
    print("Preaparando para enviar correo a los clientes !!!")

    # Template ID:4662150
    #"MAIL_JET_USER": "1d98821252f0e19ba4d9a6f678a8f129",
    #"MAIL_JET_PASSWORD": "4b7f29ef7df2f51dc497f3490a008cd3"

    data = {
        'FromEmail': 'facturacion@papelsa.mobi',
        'FromName': 'facturacion@papelsa.mobi',
        'Subject': 'Nueva sucursal Papel S.A.',
        'MJ-TemplateID': '4662150',
        'MJ-TemplateLanguage': 'false',
        'Recipients': [
                        {
                                #"Email": "rsanchez@papelsa.com.mx"
                                "Email": mail
                                #"Email": "jass@papelsa.com.mx"
                                #"Email": "jmorales@papelsa.com.mx"
                                #"Email": "jorge@kyo.mx"
                        }
                ]
        }
    result = mailjet.send.create(data=data)
    print(result.status_code)
    print(result.json())