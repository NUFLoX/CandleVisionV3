import time


class WatchlistManager:
    def init(self, max_age_minutes=360):
        self.watchlist = []
        self.max_age = max_age_minutes * 60  # секунды

    def add(self, signal):
        signal["added_to_watchlist_at"] = time.time()
        self.watchlist.append(signal)

    def get_all(self):
        return self.watchlist

    def clean_old(self):
        now = time.time()
        self.watchlist = [
            s for s in self.watchlist
            if now - s["added_to_watchlist_at"] < self.max_age
        ]

    def reevaluate(self, scorer_func, sentinel=None, threshold=3):
        """
        Проверяет сигналы заново.
        Если дозрел → возвращает список на вход в Executor
        """
        approved = []
        still_waiting = []

        for signal in self.watchlist:
            score, reasons = scorer_func(signal, sentinel)

            signal["score"] = score
            signal["score_reasons"] = reasons

            if score >= threshold:
                signal["status"] = "approved"
                approved.append(signal)
            else:
                still_waiting.append(signal)

        self.watchlist = still_waiting
        return approved