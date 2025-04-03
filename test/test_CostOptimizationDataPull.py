from CostOptimizationDataPull import split_inprogress_complete, DF_TYPE_DICT
import numpy as np
import pandas as pd
import pytest

class TestCostoptimizationdatapull:

    def test_split_inprogress_complete_1(self):
        """
        Test splitting dataframe into in-progress and complete dataframes.
        Verifies that records with 'complete' or 'exempt' status are moved to the complete dataframe.
        """
        # Create a sample dataframe
        data = {
            'ResourceId': ['1', '2', '3', '4'],
            'RecommendationId': ['a', 'b', 'c', 'd'],
            'DateOfSavings': [pd.Timestamp('2023-01-01')] * 4,
            'FinOpsStatus': ['Complete', 'In Progress', 'Exempt', 'Needs Research'],
            'FinOpsLastModified': [pd.Timestamp('2023-01-01')] * 4,
            'Comments': ['', '', '', ''],
            'Account': ['Acc1', 'Acc2', 'Acc3', 'Acc4'],
            'estimatedMonthlySavings': [100.0, 200.0, 300.0, 400.0],
            'Savings Type': ['Type1', 'Type2', 'Type3', 'Type4'],
            'Cost Center': ['CC1', 'CC2', 'CC3', 'CC4'],
            'Service Group': ['SG1', 'SG2', 'SG3', 'SG4'],
            'Optimization Exemption': ['', '', '', ''],
            'Resource ID + Type': ['1T1', '2T2', '3T3', '4T4']
        }
        df = pd.DataFrame(data)
        df = df.astype(DF_TYPE_DICT)

        # Call the function
        idf, cdf = split_inprogress_complete(df)

        # Assertions
        assert len(idf) == 2, "In-progress dataframe should have 2 records"
        assert len(cdf) == 2, "Complete dataframe should have 2 records"

        assert all(status.lower() in ['complete', 'exempt'] for status in cdf['FinOpsStatus']), \
            "All records in complete dataframe should have 'complete' or 'exempt' status"

        assert all(status.lower() not in ['complete', 'exempt'] for status in idf['FinOpsStatus']), \
            "No records in in-progress dataframe should have 'complete' or 'exempt' status"

        assert set(idf['ResourceId']) == {'2', '4'}, "In-progress dataframe should contain ResourceIds 2 and 4"
        assert set(cdf['ResourceId']) == {'1', '3'}, "Complete dataframe should contain ResourceIds 1 and 3"

        # Check that the dataframes have the correct column types
        for col, dtype in DF_TYPE_DICT.items():
            assert idf[col].dtype == dtype, f"Column {col} in idf has incorrect dtype"
            assert cdf[col].dtype == dtype, f"Column {col} in cdf has incorrect dtype"

    def test_split_inprogress_complete_2(self):
        """
        Test split_inprogress_complete function when FinOpsStatus is neither 'complete' nor 'exempt'.
        """
        # Arrange
        input_data = {
            "ResourceId": ["res1", "res2", "res3"],
            "RecommendationId": ["rec1", "rec2", "rec3"],
            "DateOfSavings": [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02"), pd.Timestamp("2023-01-03")],
            "FinOpsStatus": ["in_progress", "pending", "started"],
            "FinOpsLastModified": [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02"), pd.Timestamp("2023-01-03")],
            "Comments": ["comment1", "comment2", "comment3"],
            "Account": ["acc1", "acc2", "acc3"],
            "estimatedMonthlySavings": [100.0, 200.0, 300.0],
            "Savings Type": ["type1", "type2", "type3"],
            "Cost Center": ["cc1", "cc2", "cc3"],
            "Service Group": ["sg1", "sg2", "sg3"],
            "Optimization Exemption": ["", "", ""],
            "Resource ID + Type": ["res1_type1", "res2_type2", "res3_type3"]
        }
        df = pd.DataFrame(input_data)
        df = df.astype(DF_TYPE_DICT)

        # Act
        idf, cdf = split_inprogress_complete(df)

        # Assert
        assert len(idf) == 3
        assert len(cdf) == 0
        assert idf.equals(df)
        assert cdf.empty
        assert list(idf.columns) == list(df.columns)
        assert list(cdf.columns) == list(df.columns)

        for index, row in idf.iterrows():
            assert row["FinOpsStatus"] in ["in_progress", "pending", "started"]

    def test_split_inprogress_complete_all_columns_present(self):
        """
        Test split_inprogress_complete when not all expected columns are present
        """
        incomplete_df = pd.DataFrame({
            'ResourceId': ['1', '2'],
            'FinOpsStatus': ['complete', 'in progress'],
            # Missing other expected columns
        })
        with pytest.raises(KeyError):
            split_inprogress_complete(incomplete_df)

    def test_split_inprogress_complete_case_sensitivity(self):
        """
        Test split_inprogress_complete for case sensitivity in FinOpsStatus
        """
        case_df = pd.DataFrame({
            'ResourceId': ['1', '2', '3'],
            'FinOpsStatus': ['COMPLETE', 'Exempt', 'in progress'],
        })
        idf, cdf = split_inprogress_complete(case_df)
        assert len(idf) == 1
        assert len(cdf) == 2

    def test_split_inprogress_complete_empty_dataframe(self):
        """
        Test split_inprogress_complete with an empty DataFrame
        """
        empty_df = pd.DataFrame(columns=DF_TYPE_DICT.keys())
        idf, cdf = split_inprogress_complete(empty_df)
        assert idf.empty
        assert cdf.empty

    def test_split_inprogress_complete_extra_columns(self):
        """
        Test split_inprogress_complete with extra unexpected columns
        """
        extra_column_df = pd.DataFrame({
            **{k: ['dummy'] for k in DF_TYPE_DICT.keys()},
            'ExtraColumn': ['extra']
        })
        idf, cdf = split_inprogress_complete(extra_column_df)
        assert 'ExtraColumn' not in idf.columns
        assert 'ExtraColumn' not in cdf.columns

    def test_split_inprogress_complete_incorrect_format(self):
        """
        Test split_inprogress_complete with incorrect data format
        """
        incorrect_format_df = pd.DataFrame({
            'ResourceId': ['1', '2'],
            'FinOpsStatus': [1, 2],  # Should be string, not int
        })
        with pytest.raises(AttributeError):
            split_inprogress_complete(incorrect_format_df)

    def test_split_inprogress_complete_incorrect_type(self):
        """
        Test split_inprogress_complete with incorrect input type
        """
        with pytest.raises(AttributeError):
            split_inprogress_complete([1, 2, 3])

    def test_split_inprogress_complete_invalid_columns(self):
        """
        Test split_inprogress_complete with invalid columns
        """
        invalid_df = pd.DataFrame({'Invalid': ['complete', 'in progress']})
        with pytest.raises(KeyError):
            split_inprogress_complete(invalid_df)

    def test_split_inprogress_complete_nan_values(self):
        """
        Test split_inprogress_complete with NaN values
        """
        nan_df = pd.DataFrame({
            'ResourceId': ['1', '2'],
            'FinOpsStatus': ['complete', np.nan],
        })
        with pytest.raises(AttributeError):
            split_inprogress_complete(nan_df)