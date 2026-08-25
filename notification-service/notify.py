import pika
import sys
import os

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='email_queue', durable=True)

    def callback(ch, method, properties, body):
        try:
            print(f" [EMAIL GÖNDERİLDİ] {body.decode()}")
            # Başarılı ise RabbitMQ'ya 'Tamam, sil' diyoruz
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [HATA] Gönderilemedi: {e}")
            # Başarısız ise mesajı çöpe at (DLX'e gider)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    # auto_ack=False yapıyoruz ki manuel onaylayalım
    channel.basic_consume(queue='email_queue', on_message_callback=callback, auto_ack=False)

    print(' [*] E-Posta servisi başlatıldı. Mesajlar bekleniyor. Çıkmak için CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
