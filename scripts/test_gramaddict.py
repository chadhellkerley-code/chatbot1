from integraciones.android_sim_adapter import AndroidSimAdapter

if __name__ == "__main__":
    bot = AndroidSimAdapter("myaccount")  # mismo nombre que usaste en 'gramaddict init'
    bot.start_session(["--mode", "interact-users-list"])
