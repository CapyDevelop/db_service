import requests


def get_user_info(access_token):
    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "schoolid": "6bfe3c56-0211-4fe1-9e59-51616caac4dd",
        "Content-Type": "application/json",
        "cookie": f"tokenid={access_token}"
    }

    data = {
        "operationName": "userRoleLoaderGetRoles",

        "query": """query userRoleLoaderGetRoles {
  user {
    getCurrentUser {
      functionalRoles {
        code
        __typename
      }
      id
      studentRoles {
        id
        school {
          id
          shortName
          organizationType
          __typename
        }
        status
        __typename
      }
      userSchoolPermissions {
        schoolId
        permissions
        __typename
      }
      systemAdminRole {
        id
        __typename
      }
      businessAdminRolesV2 {
        id
        school {
          id
          organizationType
          __typename
        }
        orgUnitId
        __typename
      }
      __typename
    }
    getCurrentUserSchoolRoles {
      schoolId
      __typename
    }
    __typename
  }
}
""",
        "variables": {}
    }
    response = session.post("https://edu.21-school.ru/services/graphql", headers=headers, json=data)
    print(response.json())
    print(response.status_code)
    print(response.json()["data"]["user"]["getCurrentUser"]["id"])
    return response.json()["data"]["user"]["getCurrentUser"]["id"]
