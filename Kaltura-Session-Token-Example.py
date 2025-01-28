'''
A sample script for starting an API session, generating an auth token, running a test command, and ending the session.
The response is in XML instead of JSON, so it parses the XML response.
The XML response on session start looks like: <xml><result>asldkjlaksjdflaksjfdlkajsdf</result><executionTime>3.7908554077148E-5</executionTime></xml>
This extracts the <result> and saves it as "ks"

You don't need to edit anything in the script. It will prompt for your partner ID and Administrator secret. 
'''
import requests
import xml.etree.ElementTree as ET
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Start an API session in admin mode using the partner ID and administrator secret.
def start_session(partner_id, admin_secret):
    payload = {
        "service": "session",
        "action": "start",
        "partnerId": partner_id,
        "secret": admin_secret,
        "type": 2,  # Admin session type
        "userId": None  # Leave as None for admin sessions
    }
    
    response = requests.post("https://www.kaltura.com/api_v3/", data=payload)

    if response.status_code == 200:
        try:
            root = ET.fromstring(response.text)
            error = root.find(".//error")
            if error is not None:
                raise Exception(error.find("message").text)
            result = root.find(".//result")
            if result is not None:
                return result.text  # Directly return the session token
            raise Exception("No session token (KS) returned by the API.")
        except ET.ParseError:
            raise Exception(f"Failed to parse response XML: {response.text}")
    else:
        raise Exception(f"Failed to start session: {response.text}")

# End the session and expire the authentication token
def end_session(ks):
    payload = {
        "service": "session",
        "action": "end",
        "ks": ks
    }

    response = requests.post("https://www.kaltura.com/api_v3/", data=payload)

    if response.status_code == 200:
        try:
            root = ET.fromstring(response.text)
            error = root.find(".//error")
            if error is not None:
                raise Exception(error.find("message").text)
            print(f"{Fore.GREEN}Session ended successfully.{Style.RESET_ALL}")
        except ET.ParseError:
            raise Exception(f"Failed to parse response XML: {response.text}")
    else:
        raise Exception(f"Failed to end session: {response.text}")

# Main function to prompt for partner ID and admin secret, then start session, run SOME code, then end session
def main():
    print(f"{Fore.YELLOW}Kaltura API Session Sample Script...{Style.RESET_ALL}")
    partner_id = input(f"{Fore.CYAN}Enter your Kaltura Partner ID: {Style.RESET_ALL}").strip()
    admin_secret = input(f"{Fore.CYAN}Enter your Kaltura Administrator Secret: {Style.RESET_ALL}").strip()

    try:
        # Start session
        print(f"{Fore.YELLOW}Starting session...{Style.RESET_ALL}")
        ks = start_session(partner_id, admin_secret)
        print(f"{Fore.GREEN}Session started successfully. KS: {ks}{Style.RESET_ALL}")

        # Sample code demonstrating use of the session token. Insert your REAL code here
        print(f"{Fore.YELLOW}\nSample code using the session token:{Style.RESET_ALL}")
        sample_payload = {
            "service": "system",
            "action": "ping",
            "ks": ks
        }
        sample_response = requests.post("https://www.kaltura.com/api_v3/", data=sample_payload)

        if sample_response.status_code == 200:
            print(f"{Fore.GREEN}Sample request successful:{Style.RESET_ALL}")
            print(sample_response.text)
        else:
            print(f"{Fore.RED}Sample request failed:{Style.RESET_ALL}")
            print(sample_response.text)

        # End session
        print(f"{Fore.YELLOW}\nEnding session...{Style.RESET_ALL}")
        end_session(ks)

    except Exception as e:
        print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
