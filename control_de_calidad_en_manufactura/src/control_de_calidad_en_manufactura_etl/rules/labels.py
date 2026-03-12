"""
This module contains the business and integrity expectations for the
delayed manufacturing quality inspection feedback.

It ensures that the labels can be successfully joined back to their
original unit inspections and that the defect indicators are valid.
"""

###############################################################################
# Functions
###############################################################################

def get_identity_rules():
    """
    Rules to ensure the primary identifier is present.

    Without a valid unit identifier, a label is useless
    as it cannot be joined to our features for model training.
    """
    return [
        {
            "name": "valid_lbl_unit_id",
            "constraint": "unit_id IS NOT NULL",
            "tag": "labels"
        }
    ]


def get_feedback_integrity_rules():
    """
    Rules to validate the actual feedback content.

    Ensuring the defect indicator is a strict binary value and
    that the availability date is logically sound.
    """
    return [
        {
            "name": "valid_defect_flag",
            "constraint": "is_defective IS NOT NULL AND is_defective IN (0, 1)",
            "tag": "labels"
        },
        {
            "name": "valid_lbl_date",
            "constraint": "label_available_date IS NOT NULL",
            "tag": "labels"
        }
    ]


def get_label_rules():
    """
    Main entry point for defect labels data quality expectations.

    Aggregates all specific rule groups into a single list of dictionaries.
    """
    all_label_rules = []
    all_label_rules.extend(get_identity_rules())
    all_label_rules.extend(get_feedback_integrity_rules())
    
    return all_label_rules