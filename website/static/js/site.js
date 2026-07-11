const header = document.querySelector("header");
if (header) {
  const updateHeader = () => header.classList.toggle("scrolled", scrollY > 24);
  updateHeader();
  addEventListener("scroll", updateHeader, { passive: true });
}

const starCount = document.getElementById("gh-stars");
if (starCount) {
  const formatStars = count => count >= 1000 ? `${(count / 1000).toFixed(1)}k` : `${count}`;
  (async () => {
    try {
      const cached = JSON.parse(localStorage.getItem("gh-stars") || "null");
      if (cached && Date.now() - cached.t < 864e5) {
        starCount.textContent = formatStars(cached.n);
        return;
      }
      const response = await fetch("https://api.github.com/repos/Barre/ZeroFS");
      if (!response.ok) return;
      const count = (await response.json()).stargazers_count;
      if (typeof count === "number") {
        starCount.textContent = formatStars(count);
        localStorage.setItem("gh-stars", JSON.stringify({ n: count, t: Date.now() }));
      }
    } catch { }
  })();
}
