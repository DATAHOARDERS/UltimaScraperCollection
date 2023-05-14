from typing import Any


class OptionManager:
    def __init__(self) -> None:
        self.performer_options: OptionsFormat | None = None
        self.subscription_options: OptionsFormat | None = None
        pass

    async def create_option(
        self,
        items: list[Any],
        options_type: str,
        auto_choice: list[int | str] | int | str | bool = False,
    ):
        option = await OptionsFormat(items, options_type, auto_choice).formatter()
        return option


class OptionsFormat:
    def __init__(
        self,
        items: list[Any],
        options_type: str,
        auto_choice: list[int | str] | int | str | bool = False,
    ) -> None:
        self.items = items
        self.item_keys: list[str] = []
        self.string = ""
        self.options_type = options_type
        self.auto_choice = auto_choice
        self.final_choices = []

    async def formatter(self):
        options_type = self.options_type
        final_string = f"Choose {options_type.capitalize()}: 0 = All"
        if type(self.auto_choice) == int:
            self.auto_choice = str(self.auto_choice)

        if isinstance(self.auto_choice, str):
            self.auto_choice = [x for x in self.auto_choice.split(",") if x]
            self.auto_choice = (
                True
                if any(x in ["0", "all"] for x in self.auto_choice)
                else self.auto_choice
            )

        if isinstance(self.auto_choice, list):
            self.auto_choice = [x for x in self.auto_choice if x]

        match options_type:
            case "sites":
                self.item_keys = self.items
                my_string = " | ".join(
                    map(lambda x: f"{self.items.index(x)+1} = {x}", self.items)
                )
                final_string = f"{final_string} | {my_string}"
                self.string = final_string
                final_list = await self.choose_option()
                self.final_choices = [
                    key
                    for choice in final_list
                    for key in self.items
                    if choice.lower() == key.lower()
                ]
            case "profiles":
                self.item_keys = [x.auth_details.username for x in self.items]
                my_string = " | ".join(
                    map(
                        lambda x: f"{self.items.index(x)+1} = {x.auth_details.username}",
                        self.items,
                    )
                )
                final_string = f"{final_string} | {my_string}"
                self.string = final_string
                final_list = await self.choose_option()
                self.final_choices = [
                    key
                    for choice in final_list
                    for key in self.items
                    if choice.lower() == key.auth_details.username.lower()
                ]
            case "subscriptions":
                subscription_users = [x for x in self.items]
                self.item_keys = [x.username for x in subscription_users]
                my_string = " | ".join(
                    map(
                        lambda x: f"{subscription_users.index(x)+1} = {x.username}",
                        subscription_users,
                    )
                )
                final_string = f"{final_string} | {my_string}"
                self.string = final_string
                final_list = await self.choose_option()
                self.final_choices = [
                    key
                    for choice in final_list
                    for key in subscription_users
                    if choice.lower() == key.username.lower()
                ]

            case "contents":
                self.item_keys = self.items
                my_string = " | ".join(
                    map(lambda x: f"{self.items.index(x)+1} = {x}", self.items)
                )
                final_string = f"{final_string} | {my_string}"
                self.string = final_string
                final_list = await self.choose_option()
                self.final_choices = [
                    key
                    for choice in final_list
                    for key in self.items
                    if choice.lower() == key.lower()
                ]
            case "medias":
                self.item_keys = self.items
                my_string = " | ".join(
                    map(lambda x: f"{self.items.index(x)+1} = {x}", self.items)
                )
                final_string = f"{final_string} | {my_string}"
                self.string = final_string
                final_list = await self.choose_option()
                self.final_choices = [
                    key
                    for choice in final_list
                    for key in self.items
                    if choice.lower() == key.lower()
                ]
            case _:
                final_list = []
        return self

    async def choose_option(self):
        def process_option(input_values: list[str]):
            input_list_2: list[str] = []
            for input_value in input_values:
                if input_value.isdigit():
                    try:
                        input_list_2.append(self.item_keys[int(input_value) - 1])
                    except IndexError:
                        continue
                else:
                    x = [x for x in self.item_keys if x.lower() == input_value.lower()]
                    input_list_2.extend(x)
            return input_list_2

        input_list: list[str] = [x.lower() for x in self.item_keys]
        final_list: list[str] = []
        if self.auto_choice:
            if not self.scrape_all():
                if isinstance(self.auto_choice, list):
                    input_values = [str(x).lower() for x in self.auto_choice]
                    input_list = process_option(input_values)
        else:
            print(self.string)
            input_value = input().lower()
            if input_value != "0" and input_value != "all":
                input_values = input_value.split(",")
                input_list = process_option(input_values)
        final_list = input_list
        return final_list

    def scrape_all(self):
        status = False
        if (
            self.auto_choice == True
            or isinstance(self.auto_choice, list)
            and isinstance(self.auto_choice[0], str)
            and (
                self.auto_choice[0].lower() == "all"
                or self.auto_choice[0].lower() == "0"
            )
        ):
            status = True
        return status

    def return_auto_choice(self):
        identifiers = []
        if isinstance(self.auto_choice, list):
            identifiers = [x for x in self.auto_choice if not isinstance(x, bool)]
        return identifiers
