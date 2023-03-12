# Belongs to onlyfans.py (OnlyFansDataScraper Class)
# async def process_mass_messages(
#     self, authed: create_auth, mass_messages: list[create_message]
# ):
#     def compare_message(queue_id, remote_messages):
#         for message in remote_messages:
#             if "isFromQueue" in message and message["isFromQueue"]:
#                 if queue_id == message["queueId"]:
#                     return message
#                 print
#         print

#     global_found = []
#     chats = []
#     api = authed.get_api()
#     site_settings = api.get_site_settings()
#     config = api.config
#     if not (config and site_settings):
#         return
#     settings = config.settings
#     salt = settings.random_string
#     encoded = f"{salt}"
#     encoded = encoded.encode("utf-8")
#     hash = hashlib.md5(encoded).hexdigest()
#     profile_directory = authed.directory_manager.profile.metadata_directory
#     mass_message_path = profile_directory.joinpath("Mass Messages.json")
#     chats_path = profile_directory.joinpath("Chats.json")
#     if os.path.exists(chats_path):
#         chats = main_helper.import_json(chats_path)
#     date_object = datetime.today()
#     date_string = date_object.strftime("%d-%m-%Y %H:%M:%S")
#     for mass_message in mass_messages:
#         if "status" not in mass_message:
#             mass_message["status"] = ""
#         if "found" not in mass_message:
#             mass_message["found"] = {}
#         if "hashed_ip" not in mass_message:
#             mass_message["hashed_ip"] = ""
#         mass_message["hashed_ip"] = mass_message.get("hashed_ip", hash)
#         mass_message["date_hashed"] = mass_message.get("date_hashed", date_string)
#         if mass_message["isCanceled"]:
#             continue
#         queue_id = mass_message["id"]
#         text = mass_message["textCropped"]
#         text = html.unescape(text)
#         mass_found = mass_message["found"]
#         media_type = mass_message.get("mediaType")
#         media_types = mass_message.get("mediaTypes")
#         if mass_found or (not media_type and not media_types):
#             continue
#         identifier = None
#         if chats:
#             list_chats = chats
#             for chat in list_chats:
#                 identifier = chat["identifier"]
#                 messages = chat["messages"]["list"]
#                 mass_found = compare_message(queue_id, messages)
#                 if mass_found:
#                     mass_message["found"] = mass_found
#                     mass_message["status"] = True
#                     break
#         if not mass_found:
#             list_chats = authed.search_messages(text=text, limit=2)
#             if not list_chats:
#                 continue
#             for item in list_chats["list"]:
#                 user = item["withUser"]
#                 identifier = user["id"]
#                 messages = []
#                 print("Getting Messages")
#                 keep = ["id", "username"]
#                 list_chats2 = [x for x in chats if x["identifier"] == identifier]
#                 if list_chats2:
#                     chat2 = list_chats2[0]
#                     messages = chat2["messages"]["list"]
#                     messages = authed.get_messages(
#                         identifier=identifier, resume=messages
#                     )
#                     for message in messages:
#                         message["withUser"] = {k: item["withUser"][k] for k in keep}
#                         message["fromUser"] = {
#                             k: message["fromUser"][k] for k in keep
#                         }
#                     mass_found = compare_message(queue_id, messages)
#                     if mass_found:
#                         mass_message["found"] = mass_found
#                         mass_message["status"] = True
#                         break
#                 else:
#                     item2 = {}
#                     item2["identifier"] = identifier
#                     item2["messages"] = authed.get_messages(identifier=identifier)
#                     chats.append(item2)
#                     messages = item2["messages"]["list"]
#                     for message in messages:
#                         message["withUser"] = {k: item["withUser"][k] for k in keep}
#                         message["fromUser"] = {
#                             k: message["fromUser"][k] for k in keep
#                         }
#                     mass_found = compare_message(queue_id, messages)
#                     if mass_found:
#                         mass_message["found"] = mass_found
#                         mass_message["status"] = True
#                         break
#                     print
#                 print
#             print
#         if not mass_found:
#             mass_message["status"] = False
#     main_helper.export_json(chats, chats_path)
#     for mass_message in mass_messages:
#         found = mass_message["found"]
#         if found and found["media"]:
#             user = found["withUser"]
#             identifier = user["id"]
#             print
#             date_hashed_object = datetime.strptime(
#                 mass_message["date_hashed"], "%d-%m-%Y %H:%M:%S"
#             )
#             next_date_object = date_hashed_object + timedelta(days=1)
#             print
#             if mass_message["hashed_ip"] != hash or date_object > next_date_object:
#                 print("Getting Message By ID")
#                 x = await authed.get_message_by_id(
#                     identifier=identifier, identifier2=found["id"], limit=1
#                 )
#                 new_found = x["result"]["list"][0]
#                 new_found["withUser"] = found["withUser"]
#                 mass_message["found"] = new_found
#                 mass_message["hashed_ip"] = hash
#                 mass_message["date_hashed"] = date_string
#             global_found.append(found)
#         print
#     print
#     main_helper.export_json(mass_messages, mass_message_path)
#     return global_found
