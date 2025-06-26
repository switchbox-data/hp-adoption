#!/usr/bin/env python

########################################################################################
## masssave_reader.py
## Switchbox
## David Karp
## 2025-06-12
##
## This is a function to access some data MassSave
## https://viewer.dnv.com/macustomerprofile/entity/1444/report/2078
##
## Currently it only supports Residential: Electrification data##
##
########################################################################################


import json
from datetime import datetime
from typing import Any

import polars as pl
import requests
from attrs import define, field
from attrs.validators import in_

SHARED_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://app.powerbi.com",
    "referer": "https://app.powerbi.com/",
    "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "x-powerbi-hostenv": "Embed for Customers",
}

COLUMNS_TO_SOURCES = {
    "Suppression option": "s",
    "Year": "d",
    "Heat_pump": "d1",
    "YEAR": "a",
    "Sector": "d21",
    "Is_track_new_meas": "f",
    "Suppression status": "s1",
    "Option": "s2",
    "New_Construction_Rule": "n",
    "Suppression_visible": "s3",
}
OPERATORS = ["In", "Less_than", "Greater_than"]


@define()
class MassSaveFilter:
    column: str = field(validator=in_(COLUMNS_TO_SOURCES.keys()))
    values: list[str] = field()
    operator: str = field(default="In", validator=in_(OPERATORS))
    invert: bool = field(default=False)

    @classmethod
    def show_columns(cls) -> list[str]:
        return list(COLUMNS_TO_SOURCES.keys())  # todo, this should be stored in a nicer way

    def to_dict(self) -> dict:
        match self.operator:
            case "In":
                cond = {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": COLUMNS_TO_SOURCES[self.column]}},
                                    "Property": self.column,
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": f"'{v}'"}} for v in self.values]],
                    }
                }
            case "Greater_than":
                assert len(self.values) == 1, "Greater than filter must have exactly one value"  # noqa: S101
                cond = {
                    "Comparison": {
                        "ComparisonKind": 2,
                        "Left": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": COLUMNS_TO_SOURCES[self.column]}},
                                "Property": self.column,
                            }
                        },
                        "Right": {"Literal": {"Value": self.values[0]}},
                    }
                }
            case "Less_than":
                assert len(self.values) == 1, "Less than filter must have exactly one value"  # noqa: S101
                cond = {
                    "Comparison": {
                        "ComparisonKind": 2,
                        "Left": {"Literal": {"Value": self.values[0]}},
                        "Right": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": COLUMNS_TO_SOURCES[self.column]}},
                                "Property": self.column,
                            }
                        },
                    }
                }
            case _:
                raise ValueError(f"Invalid operator: {self.operator}")  # noqa: TRY003
        if self.invert:
            return {"Condition": {"Not": cond}}
        return {"Condition": cond}


@define()
class MassSaveQuery:
    filters: list[MassSaveFilter] = field(default=None)
    endpoint_url: str = field(
        default="https://wabi-north-europe-e-primary-redirect.analysis.windows.net/explore/querydata?synchronous=true"
    )
    payload: dict = field(init=False)

    def __attrs_post_init__(self) -> None:
        self.payload = {"version": "1.0.0", "cancelQueries": [], "modelId": 2256951, "userPreferredLocale": "en-US"}

    def _create_filters(self, filters: list[dict]) -> list[dict]:
        if filters is None:
            return ""
        return filters

    def _create_query(self) -> dict[str, Any]:
        query = {
            "Version": 2,
            "From": [
                {"Name": "d2", "Entity": "Dim_City", "Type": 0},
                {"Name": "f", "Entity": "Fact", "Type": 0},
                {"Name": "s", "Entity": "Suppression table", "Type": 0},
                {"Name": "d", "Entity": "Dim_Year", "Type": 0},
                {"Name": "d1", "Entity": "Dim_Heat_pump", "Type": 0},
                {"Name": "a", "Entity": "ACS", "Type": 0},
                {"Name": "d21", "Entity": "Dim_Sector", "Type": 0},
                {"Name": "s1", "Entity": "Suppression global switch", "Type": 0},
                {"Name": "s2", "Entity": "Suppression style", "Type": 0},
                {"Name": "n", "Entity": "New Construction Rule", "Type": 0},
                {"Name": "s3", "Entity": "Suppression display control", "Type": 0},
            ],
            "Select": [
                {
                    "Column": {"Expression": {"SourceRef": {"Source": "d2"}}, "Property": "City"},
                    "Name": "Dim_City.City",
                    "NativeReferenceName": "Municipality",
                },
                {
                    "Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Participants"},
                    "Name": "Fact.Participants",
                    "NativeReferenceName": "Installed heat pumps (accounts)1",
                },
                {
                    "Measure": {"Expression": {"SourceRef": {"Source": "f"}}, "Property": "Participanting locations"},
                    "Name": "Fact.Participanting locations",
                    "NativeReferenceName": "Installed heat pumps (locations)",
                },
            ],
            "Where": [f.to_dict() for f in self.filters] if self.filters is not None else [],
            "OrderBy": [
                {
                    "Direction": 1,
                    "Expression": {"Column": {"Expression": {"SourceRef": {"Source": "d2"}}, "Property": "City"}},
                }
            ],
        }
        return {
            "version": "1.0.0",
            "queries": [
                {
                    "Query": {
                        "Commands": [
                            {
                                "SemanticQueryDataShapeCommand": {
                                    "Query": query,
                                    "Binding": {
                                        "Primary": {"Groupings": [{"Projections": [0, 1, 2], "Subtotal": 1}]},
                                        "DataReduction": {"DataVolume": 3, "Primary": {"Window": {"Count": 500}}},
                                        "Version": 1,
                                    },
                                    "ExecutionMetricsKind": 1,
                                }
                            }
                        ]
                    },
                    "QueryId": "",
                    "ApplicationContext": {
                        "DatasetId": "35b53c35-e590-4b77-8b00-b6cf403eff38",
                        "Sources": [
                            {"ReportId": "bdc9170f-1a7e-44d2-be94-98e07915d04c", "VisualId": "ecc58064d3cee7a05d02"}
                        ],
                    },
                }
            ],
            "cancelQueries": [],
            "modelId": 2256951,
            "userPreferredLocale": "en-US",
        }

    @staticmethod
    def _json_to_df(data: dict) -> pl.DataFrame:
        data_array = data["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][1]["DM1"]

        # Create a list of dictionaries for each city
        rows = []
        for item in data_array:
            if "C" in item and len(item["C"]) >= 3:  # Only process items with all three values
                rows.append({
                    "municipality": item["C"][0],
                    "installed_hp_accounts": int(item["C"][1].rstrip("L")),  # Remove 'L' and convert to int
                    "installed_hp_locations": int(item["C"][2].rstrip("L")),  # Remove 'L' and convert to int
                })

        # Create DataFrame - keep municipality as a regular column
        return pl.DataFrame(rows)

    def run_query(self, token: str, return_type: str = "df") -> dict | pl.DataFrame:
        headers = SHARED_HEADERS | {"authorization": f"EmbedToken {token}"}

        # Create and send the query
        query_payload = self._create_query()

        response = requests.post(self.endpoint_url, headers=headers, json=query_payload, timeout=30)

        if response.status_code == 200:
            content = response.text.encode("utf-8").decode("utf-8-sig")  # Remove BOM
            data = json.loads(content)
            return self.__class__._json_to_df(data) if return_type == "df" else data
        else:
            # todo, this should be a proper error
            print(f"Error: {response.status_code}")
            print(response.text)
            return {}


if __name__ == "__main__":
    date = datetime.now().strftime("%Y%m%d")
    outfile = f"masssave_hpinstalls_{date}.csv"

    # TODO: add a way to get the token from the user
    msq = MassSaveQuery(filters=[MassSaveFilter(column="Heat_pump", values=["Energy efficiency"], operator="In")])

    ### These expire in O(10s of mins); get fresh one from developer tools tab from a query in
    ### https://viewer.dnv.com/macustomerprofile/entity/1444/report/2078
    token = """
    H4sIAAAAAAAEAB2Ut66EVgBE_-W1WCIsYbHkgrjknC4dOcOSg-V_97P7KY6OZubvHyu5-ynJf_782aA1xuRhUCPMfVn3J87SnGPGg7sg_d6_2idD63BBLc3YPNbwRF-MbzYFDxDUHV2pEUp5sctpnomrlsFdgysaArjJlO9i6QpNuJLwSH0dB0WMY_a5ueBUm8Pyi6eUi38LZn6qaTy782g-PMRmkczvRtpRQdtjYVVLl1Nw-sNMaOnPxrtNCRuvIrprxm31EYGXrdPPXYwGL83fXXmdz8IEsMU_QoqEJSryrHkHsz9ynWvUk1sLzoRmNxmerN8jqSdtdlPi4bUYDc9ejYo2aGuYPLdDGbV6HoXZ7zFiIrztG0rWMFSUh_tEEqXlvmsKF70czos5T3pLCyOJ6QwQgFL0ZQuSchPfzSljDEDsCUQDEtB7QE3TIZredvqvsUB5AGjKU7M3y1lVGyvnZh4cKiuFk_NweEwM_dgm0XRBZZaBqBNYjjDvBT9TtMg-BP9pzDtVpDYsBp4833EaZkK_lr7oB8-M8dpNM-nbj7VLGu7lTobWfulbI8_Yjk-VUmj26J8yVF4QJmol1dmXDTvfgOMZcO591FBIG2--U2xSQMZmlHZM3TOf2ML6uqpqz7-JjFIs5t3MWHJT4j0lKfwYwzfWdMTKfB4jBtJ9x9n57GGNyqJ07QUqJqLSTEJJVnCtIOvnOHcCK10Gl-58OT6eD1jtyJcsTSyMjT2XaTMrlLmbeSu5uEl6C6jChhEEwekJUiWS2RwIJaIygnfmumK054VXCh0vYu2lBBmLBBfAfHtfmxR0umyxb9fRtLOoEyCBZUzScRMk5O2BsG1G5gNJtexESJJvCox4rIaj-fIcuV0Or8jEN84eSMCI78X1R6NzL9rBNAM0ym87r1WqoaCoGdid7D6j-qPc1R3eE3NY0UDUds1ZoIhqJRcP8V1dFW9n_Y0tpxf4-eOHW-7vNqnF_TvHgGik1p8hfAJZK1aRjSYcsM-2Q8uMaxer2Y7TbkJ6KToVFmMTPYfGD-Y2PuEkfR2HjIdlFkvm0f4u2TIxGEJFrwMd8kyLJvI3B1kNMEYYC8W-e0wSuD7WNtQgGm_REQykzYSBpnjS_ARBVmGWLxiVI_6ySRnh2IEcTMFGPTXmKngetkIAZ5L5TviLgx5N2Y1j0icv9w2cDXv15TXMxIAMiEnTw-JVE4HEzJwQlRgZvz5RSJhCWcQWj4blM-ZTF5OD78zaeuY2lCfcqqF019uKhTV3cKb8yPH4RCkPbrJQ4CGz2S2nMqSlxkxNJim17othSMGoDsejlHwqiqgO3U_ByPz113-a729dLHLwa1nfbHGkk1ZZAkz6Zvgng_f8_D_lNtWYbPtS_MaMeTelQP-9uI6t40hbbquoxEd7dYg7RP1gfufU_ggTxQk7zXk3xIsUFWMj-dF-IXOr8u6JWJuTPKY6nHQtlBtbLasOsqIPyg9CcfcnS4fOfm13i79pmqzQR8UXV_G-7dcBrTag8lACge4HJ6ze6KqAQUnVkLXNxEOXqBakx1VCUT5NnCH3691vrX2kYR8FVJ2_dDiEh0N_53nwxLkDohJXVf4dFfqVcw5b6pI8-VdE9uU1pEJXzNT5MRVed2VW5FJf6l585A95Ka4uxAtHbEJ7dMtV1kDbI5mAliCyWvdtkZwCyMNq435fo7kO8I6EYju4_Lpl-v5bQFPIMclH4H06S-1fzf_8CxpVXNWCBgAA.eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly9XQUJJLU5PUlRILUVVUk9QRS1FLVBSSU1BUlktcmVkaXJlY3QuYW5hbHlzaXMud2luZG93cy5uZXQiLCJleHAiOjE3NTA5NTY0ODksInByaXZhdGVMaW5rc0VuYWJsZWQiOnRydWUsImFsbG93QWNjZXNzT3ZlclB1YmxpY0ludGVybmV0Ijp0cnVlfQ==
    """  # noqa: S105

    # TODO: run all permutations of filters and concat
    data = msq.run_query(token)

    data.write_csv(outfile)
