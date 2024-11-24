import java.util.Objects;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubEventData {
    private String id;
    private String type;
    private Actor actor;
    private Repo repo;
    private Map<String, Object> payload; // Optional, based on event type
    
    @JsonProperty("created_at")
    private String createdAt;

    public GithubEventData() {};

    public GithubEventData(String id, String type, Actor actor, Repo repo, Map<String, Object> payload, String createdAt) {
        this.id = id;
        this.type = type;
        this.repo = repo;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Actor getActor() {
        return actor;
    }

    public void setActor(Actor actor) {
        this.actor = actor;
    }

    public Repo getRepo() {
        return repo;
    }

    public void setRepo(Repo repo) {
        this.repo = repo;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    // Nested classes
    public static class Actor {
        private String id;
        private String login;

        @JsonProperty("display_login")
        private String displayLogin;

        @JsonProperty("gravatar_id")
        private String gravatarId;

        private String url;

        @JsonProperty("avatar_url")
        private String avatarUrl;

        public Actor() {}

        public Actor(String id, String login, String displayLogin, String gravatarId, String url, String avatarUrl) {
            this.id = id;
            this.login = login;
            this.displayLogin = displayLogin;
            this.gravatarId = gravatarId;
            this.url = url;
            this.avatarUrl = avatarUrl;
        }

        // Getters and Setters
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getLogin() {
            return login;
        }

        public void setLogin(String login) {
            this.login = login;
        }

        public String getDisplayLogin() {
            return displayLogin;
        }

        public void setDisplayLogin(String displayLogin) {
            this.displayLogin = displayLogin;
        }

        public String getGravatarId() {
            return gravatarId;
        }

        public void setGravatarId(String gravatarId) {
            this.gravatarId = gravatarId;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getAvatarUrl() {
            return avatarUrl;
        }

        public void setAvatarUrl(String avatarUrl) {
            this.avatarUrl = avatarUrl;
        }
    }

    public static class Repo {
        private String id;
        private String name;
        private String url;

        public Repo() {}

        public Repo(String id, String name, String url) {
            this.id = id;
            this.name = name;
            this.url = url;
        }

        // Getters and Setters
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb  = new StringBuilder("GithubEventData{");
        sb.append("id=").append(id).append('\'');
        sb.append(", type=").append(type).append('\'');
        sb.append(", repo=").append(repo.getName()).append('\'');
        return sb.toString();
    }

    public int hashCode() {
        return Objects.hash(super.hashCode(), id, type, createdAt);
    }
}
