########################################################################################
## masssave_reader.py
## SwitchBox
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
