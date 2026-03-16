"""
This module contains all the business and integrity expectations
for the stream of manufacturing quality control inspections.

Rules are divided into logical domains to facilitate auditing
and maintenance by the data engineering and quality assurance teams.
"""


# ----------------- Functions -----------------


def get_identity_and_time_rules():
    """
    Rules to ensure primary identifiers, foreign keys, and temporal boundaries 
    are present and logically valid for downstream joins and aggregations.
    """
    return [
        {
            "name": "valid_unit_id",
            "constraint": "unit_id IS NOT NULL",
            "tag": "inspections"
        },
        {
            "name": "valid_timestamp",
            "constraint": "timestamp IS NOT NULL",
            "tag": "inspections"
        },
        {
            "name": "valid_machine_id",
            "constraint": "machine_id IS NOT NULL",
            "tag": "inspections"
        },
        {
            "name": "valid_material_batch_id",
            "constraint": "material_batch_id IS NOT NULL",
            "tag": "inspections"
        }
    ]


def get_sensor_and_process_rules():
    """
    Rules to validate the physical sensor readings and process metrics
    associated with the manufacturing unit.
    """
    return [
        {
            "name": "valid_temperature",
            "constraint": "temperature_celsius IS NOT NULL AND temperature_celsius >= 220 AND temperature_celsius <= 310",
            "tag": "inspections"
        },
        {
            "name": "valid_pressure",
            "constraint": "pressure_bar IS NOT NULL AND pressure_bar >= 2.0 AND pressure_bar <= 8.0",
            "tag": "inspections"
        },
        {
            "name": "valid_vibration",
            "constraint": "vibration_mm_s IS NOT NULL AND vibration_mm_s >= 0.5 AND vibration_mm_s <= 15.0",
            "tag": "inspections"
        },
        {
            "name": "valid_voltage",
            "constraint": "voltage_v IS NOT NULL AND voltage_v >= 3.0 AND voltage_v <= 3.6",
            "tag": "inspections"
        },
        {
            "name": "valid_current",
            "constraint": "current_ma IS NOT NULL AND current_ma >= 60 AND current_ma <= 220",
            "tag": "inspections"
        },
        {
            "name": "valid_humidity",
            "constraint": "humidity_pct IS NOT NULL AND humidity_pct >= 20 AND humidity_pct <= 80",
            "tag": "inspections"
        },
        {
            "name": "valid_particle_count",
            "constraint": "particle_count_m3 IS NOT NULL AND particle_count_m3 >= 100 AND particle_count_m3 <= 8000",
            "tag": "inspections"
        },
        {
            "name": "valid_solder_thickness",
            "constraint": "solder_thickness_um IS NOT NULL AND solder_thickness_um >= 60 AND solder_thickness_um <= 220",
            "tag": "inspections"
        },
        {
            "name": "valid_alignment_error",
            "constraint": "alignment_error_um IS NOT NULL AND alignment_error_um >= 0.1 AND alignment_error_um <= 50.0",
            "tag": "inspections"
        },
        {
            "name": "valid_optical_density",
            "constraint": "optical_density IS NOT NULL AND optical_density >= 0.0 AND optical_density <= 1.0",
            "tag": "inspections"
        },
        {
            "name": "valid_tool_wear",
            "constraint": "tool_wear_pct IS NOT NULL AND tool_wear_pct >= 0 AND tool_wear_pct <= 100",
            "tag": "inspections"
        },
        {
            "name": "valid_production_speed",
            "constraint": "production_speed_pct IS NOT NULL AND production_speed_pct >= 60 AND production_speed_pct <= 130",
            "tag": "inspections"
        }
    ]


def get_contextual_and_categorical_rules():
    """
    Rules to validate categorical indicators, shift information,
    supplier consistency, and defect flags.
    """
    return [
        {
            "name": "valid_shift",
            "constraint": "shift IS NOT NULL AND shift IN ('morning', 'afternoon', 'night')",
            "tag": "inspections"
        },
        {
            "name": "valid_supplier_id",
            "constraint": "supplier_id IS NOT NULL AND supplier_id IN ('SUP_ALPHA', 'SUP_BETA', 'SUP_GAMMA', 'SUP_DELTA')",
            "tag": "inspections"
        },
        {
            "name": "valid_line_id",
            "constraint": "line_id IS NOT NULL AND line_id IN ('LINE_A', 'LINE_B', 'LINE_C', 'LINE_D')",
            "tag": "inspections"
        },
        {
            "name": "valid_machine_format",
            "constraint": "machine_id LIKE 'MCH_%'",
            "tag": "inspections"
        }
    ]


def get_inspection_rules():
    """
    Main entry point for manufacturing inspection data quality expectations.

    Aggregates all specific rule groups into a single list of dictionaries.
    """
    all_inspection_rules = []
    all_inspection_rules.extend(get_identity_and_time_rules())
    all_inspection_rules.extend(get_sensor_and_process_rules())
    all_inspection_rules.extend(get_contextual_and_categorical_rules())
    
    return all_inspection_rules