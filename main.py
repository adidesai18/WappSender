from flask import Flask, request, jsonify
import requests
import os
import firebase_admin
from dotenv import load_dotenv
from firebase_admin import credentials, firestore
from firebase_admin.firestore import firestore as fs
import json
import time
import logging
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
executor = ThreadPoolExecutor(max_workers=8)

wappsender = os.getenv('wappsender')
bot_token = os.getenv('bot_token')
telegram_api_url = f"https://api.telegram.org/bot{bot_token}"
instance = os.getenv('instance')
wapp_token = os.getenv('wapp_token')
private_key=os.getenv('private_key')
private_key_id=os.getenv('private_key_id')
client_email=os.getenv('client_email')
project_id=os.getenv('project_id')
client_id=os.getenv('client_id')
client_x509_cert_url=os.getenv('client_x509_cert_url')

cred_dict = {
  "type": "service_account",
  "project_id": project_id,
  "private_key_id": private_key_id,
  "private_key": private_key,
  "client_email": client_email,
  "client_id": client_id,
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": client_x509_cert_url,
  "universe_domain": "googleapis.com"
}

cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)
db = firestore.client()

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

class WappSenderError(Exception):
    pass

login_op={
    'login_mode':False,
    'login_users':{
    }
}

exclude_op={
    'exclude_mode':False
}

upload_content_op={
    'upload_content_mode':False,
    'content':{
        'photos':[],
        'videos':[],
        'documents':[],
    }
}

broadcast_op={
    'broadcast_mode':False,
    'main_loop_mood':False,
    'group_count':0,
    'groups_len':0,
    'terminate':False,
    'error_target':None,
    'error_target_index':None,
    'error_resend_list':None
}

bot_commands_list=['/start','/login','/upload_content','/clear_content','/broadcast','/exclude_users','/show_status','/terminate']

# -----------------------------------------------------------

def send_text(target:str,text:str):
    try:
        url = f"https://api.ultramsg.com/{instance}/messages/chat"
        payload = json.dumps({
            "to": target,
            "token": wapp_token,
            "body": text
        })
        response = requests.request("POST", url, headers={'Content-Type': 'application/json'}, data=payload, timeout=10)
        logging.info(response.text)
    except Exception as e:
        broadcast_op['error_target']=target
        raise WappSenderError(f'{e} - in send_text()')

def send_image(target:str,cap:str,link:str):
    try:
        url = f"https://api.ultramsg.com/{instance}/messages/image"
        payload = json.dumps({
            "to": target,
            "image": link,
            "token": wapp_token,
            "caption": cap,
        })
        response = requests.request("POST", url, headers={'Content-Type': 'application/json'}, data=payload, timeout=10)
        logging.info(response.text)
    except Exception as e:
        broadcast_op['error_target']=target
        raise WappSenderError(f'{e} - in send_image()')

def send_video(target:str,cap:str,link:str):
    try:
        url = f"https://api.ultramsg.com/{instance}/messages/video"
        payload = json.dumps({
            "to": target,
            "token": wapp_token,
            "video": link,
            "caption": cap,
        })
        response = requests.request("POST", url, headers={'Content-Type': 'application/json'}, data=payload, timeout=10)
        logging.info(response.text)
    except Exception as e:
        broadcast_op['error_target']=target
        raise WappSenderError(f'{e} - in send_video()')

def send_document(target:str,cap:str,link:str,docname:str):
    try:
        url = f"https://api.ultramsg.com/{instance}/messages/document"
        payload = json.dumps({
            "token": wapp_token,
            "to": target,
            "filename": docname,
            "document": link,
            "caption": cap,
        })
        response = requests.request("POST", url, headers={'Content-Type': 'application/json'}, data=payload, timeout=10)
        logging.info(response.text)
    except Exception as e:
        broadcast_op['error_target']=target
        raise WappSenderError(f'{e} - in send_document()')

def send_to_groups(ids:list,content:dict,user_id:str):
    try:
        broadcast_op['groups_len']=len(ids)
        for id in ids:
            if broadcast_op['terminate']:
                clear_content()
                executor.submit(terminate,  user_id)
                send_txt_message(user_id,'Termination process initiated!!!!')
                break
            else:
                if content['photos']:
                    for img in content['photos']:
                        if broadcast_op['terminate']:
                            pass
                        else:
                            send_image(id,'',img)
                if content['videos']:
                    if broadcast_op['terminate']:
                        pass
                    else:
                        for vid in content['videos']:
                            send_video(id,'',vid)
                if content['documents']:
                    if broadcast_op['terminate']:
                        pass
                    else:
                        for doc_dict in content['documents']:
                            key = list(doc_dict.keys())[0]
                            value = doc_dict[key]
                            send_document(id,'',value,key)
                if 'text' in content and content['text']!=None:
                    if broadcast_op['terminate']:
                        pass
                    else:
                        send_text(id,content['text'])
                broadcast_op['group_count']+=1
    except Exception as e:
        broadcast_op['error_target_index']=ids.index(broadcast_op['error_target'])
        broadcast_op['error_resend_list']=ids[broadcast_op['error_target_index']:]
        logging.info(broadcast_op['error_resend_list'])
        raise WappSenderError(f'{e} - in send_to_groups()')

def delete_messages(msgId:str):
    try:
        url = f"https://api.ultramsg.com/{instance}/messages/delete"
        payload = json.dumps({
            "token": wapp_token,
            "msgId": msgId})
        response = requests.request("POST", url, headers={'Content-Type': 'application/json'}, data=payload, timeout=10)
        logging.info(response.text)
        return response.json()
    except Exception as e:
        raise WappSenderError(f'{e} - in delete_messages()')
    
def get_statistics():
    try:
        url = f"https://api.ultramsg.com/{instance}/messages/statistics"
        querystring = {"token": wapp_token}
        response = requests.request("GET", url, headers={'content-type': 'application/json'}, params=querystring, timeout=10)
        logging.info(response.text)
        message_stats = response.json()['messages_statistics']
        txt_message = (
            f"Statistics:\n"
            f"{'Sent:':<10} {message_stats['sent']}\n"
            f"{'Queue:':<8} {message_stats['queue']}\n"
            f"{'Unsent:':<8} {message_stats['unsent']}\n"
            f"{'Invalid:':<10} {message_stats['invalid']}\n"
            f"{'Expired:':<8} {message_stats['expired']}"
        )
        return txt_message
    except Exception as e:
        raise WappSenderError(f'{e} - in get_statistics()')
        
@lru_cache(maxsize=1)        
def get_groups_dict():
    try:
        url = f"https://api.ultramsg.com/{instance}/groups"
        querystring = {"token": wapp_token}
        response = requests.request("GET", url, headers={'Content-Type': 'application/json'}, params=querystring, timeout=10)
        groups=response.json()
        groups_dict={}
        
        for group in groups:
            groups_dict[group['id']]=group['name']
        return groups_dict
    except Exception as e:
        raise WappSenderError(f'{e} - in get_groups_dict()')

def clear_messages(status):
    try:
        url = f"https://api.ultramsg.com/{instance}/messages/clear"
        payload = json.dumps({"token": wapp_token, "status": status})
        requests.post(url, headers={'Content-Type': 'application/json'}, data=payload, timeout=10)
    except Exception as e:
        raise WappSenderError(f'{e} - in clear_messages()')

def terminate(user_id):
    try:
        clear_messages("queue")
        clear_messages("sent")
        time.sleep(10)
        value = db.collection('WappSender').document('message-ids').get().get('ids')
        for msgId in value:
            delete_messages(msgId)
        send_txt_message(user_id,'Termination process completed')
        broadcast_op['main_loop_mood'] = False
        broadcast_op['terminate']=False
    except Exception as e:
        raise WappSenderError(f'{e} - in terminate()')

@lru_cache(maxsize=1)        
def get_excluded_users():
    try:
        excluded_user = db.collection('WappSender').document('exclude_user').get().get('ids')
        return list(excluded_user)
    except Exception as e:
        raise WappSenderError(f'{e} - in get_excluded_users()')
# -----------------------------------------------------------

def bytes_to_mb(byte_size: int) -> float:
    """Convert bytes to megabytes, rounded to two decimal places."""
    return round(byte_size / (1024 * 1024), 2)

def get_file_path(id:str):
    url = f"{telegram_api_url}/getFile?file_id={id}"
    response = requests.get(url)
    file_info = response.json()
    if file_info['ok']:
        file_path = file_info['result']['file_path']
        file_url = f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
        return file_url
    else:
        raise WappSenderError(f"\n{file_info['description']}")

def send_txt_message(chat_id, text):
    try:
        payload = {
            'chat_id': chat_id,
            'text': str(text),
        }
        return requests.post(f"{telegram_api_url}/sendMessage", json=payload).json()
    except Exception as e:
        logging.error(f"Error: {e} occurred while sending the text message through the Telegram bot.")

def clear_content():

    upload_content_op['content'] = {
        'photos': [],
        'videos': [],
        'documents': [],
    }

    upload_content_op['upload_content_mode'] = False

    broadcast_op.update({
        'broadcast_mode': False,
        'group_count': 0,
        'groups_len': 0,
        'main_loop_mood': False
    })

def send_in_background(target_ids, content, user_id, success_message):
    broadcast_op['main_loop_mood'] = True
    try:
        send_to_groups(target_ids, content,user_id)
        if not broadcast_op['terminate']:
            send_text('+917020805020','Broadcast completed.')
            txt_message=get_statistics()
            send_txt_message(user_id, success_message)
            send_txt_message(user_id,txt_message)
    except Exception as e:
        send_txt_message(user_id, f'Error: {e} - in send_in_background()')
    broadcast_op['main_loop_mood'] = False
    clear_content()

def concurrent_broadcast(target_ids,content,user_id,success_message):
    broadcast_op['main_loop_mood'] = True
    try:
        if documents := content.get('documents'):
            doc_info = documents[0]
            doc_name = next(iter(doc_info))
            doc_link = doc_info[doc_name]
            caption = content.get('text', '')
            targets_string = ','.join(target_ids)
            send_document(targets_string,caption,doc_link,doc_name)
        elif content.get('videos'):
            caption = content.get('text', '') 
            targets_string = ','.join(target_ids)
            send_video(targets_string,caption,content['videos'][0])
        elif content.get('photos'):
            caption = content.get('text', '')
            targets_string = ','.join(target_ids)
            send_image(targets_string,caption,content['photos'][0])
        elif 'text' in content and content['text']!=None:
            targets_string = ','.join(target_ids)
            send_text(targets_string,content['text'])
        
        send_text('+917020805020','Broadcast completed.')
        txt_message=get_statistics()
        send_txt_message(user_id, success_message)
        send_txt_message(user_id,txt_message)
    except Exception as e:
        send_txt_message(user_id, f'Error: {e} - in concurrent_broadcast()')
    broadcast_op['main_loop_mood'] = False
    clear_content()

def terminate_in_background(user_id):
    broadcast_op['main_loop_mood'] = True
    try:
        clear_content()
        executor.submit(terminate,  user_id)
    except Exception as e:
        send_txt_message(user_id, f'Error: {e} - in terminate_in_background()')
        broadcast_op['main_loop_mood'] = False
        broadcast_op['terminate']=False

def upload_photo_in_background(update:dict,user_id:str):
    try:
        file=update['message']['photo'][-1]
        file_id=file['file_id']
        file_size=file['file_size']
        file_size_mb=bytes_to_mb(file_size)
        if file_size_mb>15.9:
            send_txt_message(user_id,f"File size too big: {file_size_mb} MB")
            return
        send_txt_message(user_id,f"Photo received: {file_size_mb} MB")
        path=get_file_path(file_id)
        upload_content_op['content']['photos'].append(path)
        logging.info(path)      
    except Exception as e:
        send_txt_message(user_id, f'Error: {e} - in upload_photo_in_background()')
    
def upload_video_in_background(update:dict,user_id:str):
    try:
        file=update['message']['video']
        file_id=file['file_id']
        file_size=file['file_size']
        file_size_mb=bytes_to_mb(file_size)
        if file_size_mb>15.9:
            send_txt_message(user_id,f"File size too big: {file_size_mb} MB")
            return
        send_txt_message(user_id,f"Video received: {file_size_mb} MB")
        path=get_file_path(file_id)
        upload_content_op['content']['videos'].append(path)
        logging.info(path)      
    except Exception as e:
        send_txt_message(user_id, f'Error: {e} - in upload_video_in_background()')
    
def upload_document_in_background(update:dict,user_id:str):
    try:
        file=update['message']['document']
        file_id=file['file_id']
        file_size=file['file_size']
        file_size_mb=bytes_to_mb(file_size)
        if file_size_mb>15.9:
            send_txt_message(user_id,f"File size too big: {file_size_mb} MB")
            return
        send_txt_message(user_id,f"Document received: {file_size_mb} MB")
        file_name=file['file_name']
        if file_name.endswith('.pdf'):
            file_name = file_name[:-4]
        path=get_file_path(file_id)
        upload_content_op['content']['documents'].append({file_name:path})         
        logging.info(path)      
    except Exception as e:
        send_txt_message(user_id, f'Error: {e} - in upload_document_in_background()')
# -----------------------------------------------------------

@app.route('/', methods=['POST'])
def webhook_post():
    update = request.json
    if 'message' in update:
        user_id = update['message']['chat']['id']
        media_keys = {"photo", "video", "document"}
        if media_keys.intersection(update['message']):
            if upload_content_op['upload_content_mode'] and not broadcast_op['main_loop_mood']:
                if 'photo' in update['message']:
                    try:
                        executor.submit(upload_photo_in_background, update, user_id)
                    except Exception as e:
                            send_txt_message(user_id, f"Error: {e} occurred during the photo upload process")
                    return jsonify({'status': 'ok'})
                        
                elif 'video' in update['message']:
                    try:
                        executor.submit(upload_video_in_background, update, user_id)
                    except Exception as e:
                            send_txt_message(user_id, f"Error: {e} occurred during the video upload process")
                    return jsonify({'status': 'ok'})
                    
                elif 'document' in update['message']:
                    try:
                        executor.submit(upload_document_in_background, update, user_id)
                    except Exception as e:
                            send_txt_message(user_id, f"Error: {e} occurred during the document upload process")
                    return jsonify({'status': 'ok'})
            
        elif 'text' in update['message']:
            text_message=update['message']['text']
            
            if not broadcast_op['main_loop_mood']:
                if text_message not in bot_commands_list:
                    if login_op['login_mode']:
                        if text_message==wappsender:
                            send_txt_message(user_id,"You are authorized to use the bot.")
                            login_op['login_users'][user_id]=True
                            login_op['login_mode']=False
                        else:
                            send_txt_message(user_id,"Invalid password. Please try again:")
                        return jsonify({'status': 'ok'})

                    elif exclude_op['exclude_mode']:
                        try:
                            list_of_strings = text_message.split(',')
                            
                            list_of_numbers = [int(num) - 1 for num in list_of_strings]
                            keys_list = list(exclude_op['groups_list'].keys())

                            for index in list_of_numbers:
                                db.collection('WappSender').document('exclude_user').update({
                                    'ids': fs.ArrayUnion([keys_list[index]])
                                })
                                
                            # Construct excluded groups message
                            excluded_groups = ''
                            get_excluded_users.cache_clear()
                            for group in get_excluded_users():
                                excluded_groups += exclude_op['groups_list'][group] + '\n'

                            send_txt_message(user_id, f'Excluded groups are:\n{excluded_groups.strip()}')
                            exclude_op['exclude_mode'] = False

                        except Exception as e:
                            send_txt_message(user_id, f"Error: {e} occurred while excluding selected groups from the main broadcast list")
                        return jsonify({'status': 'ok'})
                    
                    elif broadcast_op['broadcast_mode']:

                        if text_message == '3':
                            broadcast_op['main_loop_mood'] = True
                            try:
                                send_to_groups(['+917020805020'], upload_content_op['content'],user_id)
                                send_txt_message(user_id, 'The message has been successfully sent to Aditya.')
                            except Exception as e:         
                                send_txt_message(user_id, f'Error: {e} occurred while broadcasting content to Aditya')
                            broadcast_op['main_loop_mood'] = False
                            upload_content_op['upload_content_mode'] = True        
                            broadcast_op['broadcast_mode'] = False
                            return jsonify({'status': 'ok'})
                        
                        elif text_message == '2':
                            try:
                                excluded_users=get_excluded_users()                             
                                if excluded_users:
                                    target_list = [id for id in get_groups_dict().keys() if id not in excluded_users]
                                    doc_message = db.collection('WappSender').document('message-ids')
                                    doc_message.update({'ids': []})
                                    executor.submit(send_in_background, target_list, upload_content_op['content'], user_id, 'The message has been successfully sent to selected groups.')
                                    send_txt_message(user_id, 'Request received!.')
                                else:
                                    get_excluded_users.cache_clear()
                                    send_txt_message(user_id, 'Please use the /exclude_users command to select the groups you wish to exclude from broadcasting.')
                                    broadcast_op['broadcast_mode'] = False
                                    upload_content_op['upload_content_mode'] = True                
                            except Exception as e:
                                clear_content()
                                send_txt_message(user_id, f'Error: {e} occurred while broadcasting content to selected groups')
                            return jsonify({'status': 'ok'})

                        elif text_message == '1':
                            try:
                                target_list = list(get_groups_dict().keys())
                                doc_message = db.collection('WappSender').document('message-ids')
                                doc_message.update({'ids': []})
                                executor.submit(send_in_background, target_list, upload_content_op['content'], user_id, 'The message has been successfully sent to all groups.')
                                send_txt_message(user_id, 'Request received!.')
                            except Exception as e:
                                clear_content()
                                send_txt_message(user_id, f'Error: {e} occurred while broadcasting content to all groups')
                            return jsonify({'status': 'ok'})
                        
                        elif text_message == '0':
                            try:
                                excluded_users=get_excluded_users()
                                target_list = [id for id in get_groups_dict().keys() if id not in excluded_users]
                                doc_message = db.collection('WappSender').document('message-ids')
                                doc_message.update({'ids': []})
                                executor.submit(concurrent_broadcast, target_list, upload_content_op['content'], user_id, 'The message has been successfully sent to all groups.')
                                send_txt_message(user_id, 'Request received!.')
                            except Exception as e:
                                clear_content()
                                send_txt_message(user_id, f'Error: {e} occurred while broadcasting content to all groups')
                            return jsonify({'status': 'ok'})
                        
                    elif upload_content_op['upload_content_mode']:
                        if text_message.startswith('&'):
                            upload_content_op['content']['videos'].append(text_message.lstrip('&'))
                            send_txt_message(user_id,"Video received")
                        elif text_message.startswith('₹'):
                            upload_content_op['content']['photos'].append(text_message.lstrip('₹'))
                            send_txt_message(user_id,"Photo received")
                        else:
                            upload_content_op['content']['text']=text_message
                            send_txt_message(user_id,'Text message received!')
                        return jsonify({'status': 'ok'})
                
                elif text_message == '/upload_content':
                    if user_id not in login_op['login_users']:
                        send_txt_message(user_id,"Please log in first using /login.")
                        return jsonify({'status': 'ok'})
                    clear_content()
                    send_txt_message(user_id,'To send photos, videos, documents, or text messages, please enter the media or text you would like to broadcast.')
                    upload_content_op['upload_content_mode']=True
                    return jsonify({'status': 'ok'})

                elif text_message == '/clear_content':
                    if user_id not in login_op['login_users']:
                        send_txt_message(user_id,"Please log in first using /login.")
                        return jsonify({'status': 'ok'})
                    clear_content()
                    send_txt_message(user_id,'The media list has been successfully cleared.')
                    return jsonify({'status': 'ok'})

                elif text_message == '/broadcast':
                    if not upload_content_op['upload_content_mode']:
                        send_txt_message(user_id,"No content has been uploaded yet. Please use the /upload_content command to upload content before proceeding with the broadcast.")
                        return jsonify({'status': 'ok'})
                    upload_content_op['upload_content_mode']=False
                    send_txt_message(user_id,'1. Send a message to all groups\n2. Send a message to selected groups\n3. Send a message to Aditya')
                    broadcast_op['broadcast_mode']=True
                    return jsonify({'status': 'ok'})
                
                elif text_message == "/show_status":
                    try:
                        if user_id not in login_op['login_users']:
                            send_txt_message(user_id,"Please log in first using /login.")
                        else:
                            txt_message=get_statistics()
                            send_txt_message(user_id,txt_message)
                    except Exception as e:
                        send_txt_message(user_id, f'Error: {e} occurred in show_status()')
                    return jsonify({'status': 'ok'})
                
                elif text_message == "/login":
                    if user_id in login_op['login_users']:
                        send_txt_message(user_id,"You are already logged in.")
                        return jsonify({'status': 'ok'})
                    send_txt_message(user_id,'Please enter the password to login:')
                    login_op['login_mode']=True
                    return jsonify({'status': 'ok'})

                elif text_message == "/start":
                    if not user_id in login_op['login_users']:
                        send_txt_message(user_id, "Hello! I am WappSender,\nI am here to help you with WhatsApp broadcasting.\nTo get started, please /login to use the bot.")
                    else:
                        send_txt_message(user_id,"Welcome back! You are already logged in.")
                    return jsonify({'status': 'ok'})
                    
                elif text_message == '/exclude_users':
                    try:
                        if user_id not in login_op['login_users']:
                            send_txt_message(user_id,"Please log in first using /login.")
                            return jsonify({'status': 'ok'})
                        exclude_op['groups_list']=get_groups_dict()   
                        output_string_1 = ''
                        output_string_2=''
                        # Building the output string
                        for index, value in enumerate(exclude_op['groups_list'].values()):
                            if index<100:
                                output_string_1 += f"{index+1}:  {value}\n"
                            else:
                                output_string_2 += f"{index+1}:  {value}\n"
                        exclude_op['exclude_mode']=True
                        send_txt_message(user_id,output_string_1)
                        send_txt_message(user_id,output_string_2)
                        send_txt_message(user_id,'Please provide the indices of the groups you wish to exclude from the broadcast, separated by commas (e.g., 1,2,3).')
                    except Exception as e:
                        send_txt_message(user_id, f"Error: {e} occurred while updating the group list using the /group_list command.")
                    return jsonify({'status': 'ok'})      

                elif text_message == "/terminate":
                    try:
                        executor.submit(terminate_in_background, user_id)
                        send_txt_message(user_id,'Termination process initiated!!!!')
                    except Exception as e:
                        send_txt_message(user_id, f'Error: {e} occurred while initiating terminate_in_background()')
                        clear_content()
                    return jsonify({'status': 'ok'})         

            else:
                if text_message == "/show_status" :
                    send_txt_message(user_id, f"{broadcast_op['group_count']}/{broadcast_op['groups_len']}")
                    return jsonify({'status': 'ok'})
                
                elif text_message == "/terminate":
                    broadcast_op['terminate']=True
                    return jsonify({'status': 'ok'})
    
    return jsonify({'status': 'ok'})

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok', 'message': 'Service is healthy'}), 200

@app.route('/clear', methods=['GET'])
def cache_clear():
    get_groups_dict.cache_clear()
    get_excluded_users.cache_clear()
    excluded_user_var=get_excluded_users()
    groups_var=get_groups_dict()
    return jsonify({'Excluded_Users': excluded_user_var, 'WhatsappGroups': groups_var}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)