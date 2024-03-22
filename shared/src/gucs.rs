use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

pub trait GlobalGucSettings {
    fn telemetry_enabled(&self) -> bool;
    fn logs_enabled(&self) -> bool;
}

pub struct PostgresGlobalGucSettings {
    telemetry: GucSetting<bool>,
    logs: GucSetting<bool>,
}

impl PostgresGlobalGucSettings {
    pub const fn new() -> Self {
        Self {
            telemetry: GucSetting::<bool>::new(true),
            logs: GucSetting::<bool>::new(false),
        }
    }

    pub fn init(&self, extension_name: &str) {
        // Note that Postgres is very specific about the naming convention of variables.
        // They must be namespaced... we use 'paradedb.<variable>' below.
        // They cannot have more than one '.' - paradedb.pg_search.telemetry will not work.

        // telemetry
        GucRegistry::define_bool_guc(
            &format!("paradedb.{extension_name}_telemetry"),
            &format!("Enable telemetry on the ParadeDB {extension_name} extension.",),
            &format!("Enable telemetry on the ParadeDB {extension_name} extension.",),
            &self.telemetry,
            GucContext::Userset,
            GucFlags::default(),
        );

        // logs
        GucRegistry::define_bool_guc(
            &format!("paradedb.{extension_name}_logs"),
            "Enable logging to the paradedb.logs table?",
            "This incurs some overhead, so only recommended when debugging.",
            &self.logs,
            GucContext::Userset,
            GucFlags::default(),
        );
    }
}

impl GlobalGucSettings for PostgresGlobalGucSettings {
    fn telemetry_enabled(&self) -> bool {
        // If TELEMETRY is not 'true' at compile time, then we will never enable.
        // This is useful for test builds and CI.
        option_env!("TELEMETRY") == Some("true") && self.telemetry.get()
    }

    fn logs_enabled(&self) -> bool {
        self.logs.get()
    }
}
