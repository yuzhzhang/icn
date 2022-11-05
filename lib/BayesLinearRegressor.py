import numpy as np

class BayesLinearRegressor:
    def __init__(self, number_of_features, mean=None, cov=None, alpha=1e6, beta=1):
        # prior distribution on weights
        if mean is None:
            self.mean = np.array([[0] * (number_of_features)], dtype=float).T

        if cov is None:
            self.cov = alpha * np.identity(number_of_features)
            self.cov_inv = np.linalg.inv(self.cov)
            self.cov_init = self.cov

        self.beta = beta  # process noise
        self.number_of_features = number_of_features

    def fit(self, x, y):
        return self.update(x, y)
    
    def dilute(self, inc_alpha):
        # self.cov = self.cov + inc_alpha * np.identity*(self.number_of_features)
        self.cov = self.cov + inc_alpha * np.diag(np.diag(self.cov))
        self.cov_inv = np.linalg.inv(self.cov)

    def update(self, x, y, inc_alpha=None):
        """
        Perform a bayesian update step
        """
        if len(x.shape) == 1:
            x = x[np.newaxis, :]
        if len(y.shape) == 1:
            y = y[:, np.newaxis]

        # update state of covariance and means
        cov_n_inv = self.cov_inv + self.beta * x.T.dot(x)
        cov_n = np.linalg.inv(cov_n_inv)
        mean_n = cov_n.dot(self.cov_inv.dot(self.mean) + self.beta * x.T.dot(y))

        if inc_alpha is not None:
            # cov_n = cov_n - (cov_n - self.cov_init) * inc_alpha
            cov_n = cov_n + inc_alpha * np.identity(self.number_of_features)
            cov_n_inv = np.linalg.inv(cov_n)

        self.cov_inv = cov_n_inv
        self.cov = cov_n
        self.mean = mean_n

    def predict(self, x):
        mean = x.dot(self.mean)
        scale = np.sqrt(np.sum(x.dot(self.cov.dot(x.T)), axis=1))
        return mean, scale

    @property
    def coef_(self):
        return self.mean

    @property
    def scale_(self):
        return np.sqrt(np.diag(self.cov))
